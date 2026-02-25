import re
import time
import xml.etree.ElementTree as ET

import httpx

NCBI_RATE_LIMIT_DELAY = 0.4  # NCBI: max 3 req/sec without API key

from app.models import ResearchItem

PMID_PATTERN = re.compile(r"(?:PMID[:\s]*)(\d{5,8})", re.IGNORECASE)
PUBMED_URL_PATTERN = re.compile(
    r"pubmed\.ncbi\.nlm\.nih\.gov/(\d{5,8})",
    re.IGNORECASE,
)
PUBMED_LEGACY_URL_PATTERN = re.compile(
    r"ncbi\.nlm\.nih\.gov/pubmed/(\d{5,8})",
    re.IGNORECASE,
)
PMID_CONTEXT_PATTERN = re.compile(
    r"(?:pmid|pubmed)[^\d]{0,20}(\d{5,8})",
    re.IGNORECASE,
)
PMID_STUDY_REF_PATTERN = re.compile(
    r"(?:study|article|paper|ref\.?|reference)[^\d]{0,15}(\d{5,8})\b",
    re.IGNORECASE,
)


class PubMedClient:
    def __init__(self, tool: str, email: str | None = None) -> None:
        self._tool = tool
        self._email = email
        self._base = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"

    def extract_pmids(self, text: str) -> list[str]:
        pmids = set(PMID_PATTERN.findall(text))
        pmids.update(PUBMED_URL_PATTERN.findall(text))
        pmids.update(PUBMED_LEGACY_URL_PATTERN.findall(text))
        pmids.update(PMID_CONTEXT_PATTERN.findall(text))
        pmids.update(PMID_STUDY_REF_PATTERN.findall(text))
        return sorted(pmids)

    def fetch_study(self, pmid: str) -> ResearchItem:
        params = {
            "db": "pubmed",
            "id": pmid,
            "retmode": "json",
            "tool": self._tool,
        }
        if self._email:
            params["email"] = self._email

        with httpx.Client(timeout=20.0) as client:
            time.sleep(NCBI_RATE_LIMIT_DELAY)
            summary_response = client.get(
                f"{self._base}/esummary.fcgi",
                params=params,
            )
            summary_response.raise_for_status()
            summary_data = summary_response.json()

            time.sleep(NCBI_RATE_LIMIT_DELAY)
            pmc_response = client.get(
                f"{self._base}/elink.fcgi",
                params={
                    **params,
                    "dbfrom": "pubmed",
                    "db": "pmc",
                },
            )
            pmc_response.raise_for_status()
            pmc_data = pmc_response.json()

        result = summary_data.get("result", {}).get(pmid)
        if not isinstance(result, dict) or result.get("error"):
            err_msg = result.get("error", "not found") if isinstance(result, dict) else ""
            raise ValueError(f"PMID {pmid} not found in PubMed: {err_msg}")
        authors = [author["name"] for author in result.get("authors", [])]
        year = self._extract_year(result.get("pubdate", ""))
        full_text_url = self._extract_pmc_url(pmc_data)
        abstract = self._fetch_abstract(pmid)

        return ResearchItem(
            title=result.get("title", "").strip(),
            authors=authors,
            year=year,
            pmid=pmid,
            pmid_url=f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/",
            full_text_url=full_text_url,
            abstract=abstract,
        )

    def _fetch_abstract(self, pmid: str) -> str | None:
        """Fetch article abstract via efetch. Returns None on failure."""
        params = {
            "db": "pubmed",
            "id": pmid,
            "retmode": "xml",
            "tool": self._tool,
        }
        if self._email:
            params["email"] = self._email
        try:
            time.sleep(NCBI_RATE_LIMIT_DELAY)
            with httpx.Client(timeout=25.0) as client:
                response = client.get(
                    f"{self._base}/efetch.fcgi",
                    params=params,
                )
                response.raise_for_status()
                body = response.text
            root = ET.fromstring(body)
            parts: list[str] = []
            for elem in root.iter():
                if elem.tag.endswith("}AbstractText") or elem.tag == "AbstractText":
                    if elem.text:
                        parts.append(elem.text.strip())
            if parts:
                return " ".join(parts)[:3000]
        except Exception:
            return None
        return None

    @staticmethod
    def _sanitize_title_for_query(title: str) -> str:
        """Remove/escape chars that break PubMed query syntax."""
        t = title.strip()
        t = t.replace("[", "").replace("]", "")
        t = t.replace('"', " ").replace("'", " ")
        return re.sub(r"\s+", " ", t).strip()

    def search_pmids_by_title(
        self,
        title: str,
        max_results: int = 5,
    ) -> list[str]:
        raw = title.strip()
        if not raw:
            return []
        cleaned_title = self._sanitize_title_for_query(raw)
        if not cleaned_title:
            return []

        query_candidates = [
            f"\"{cleaned_title}\"[Title]",
            f"\"{cleaned_title}\"[Title/Abstract]",
        ]
        if "position stand" in raw.lower() and "antioxidant" in raw.lower():
            query_candidates.insert(
                0,
                '"position stand"[Title] AND antioxidants[Title]',
            )
        token_query = self._build_token_query(cleaned_title=cleaned_title)
        if token_query:
            query_candidates.append(token_query)

        collected_ids: list[str] = []
        with httpx.Client(timeout=20.0) as client:
            for query in query_candidates:
                id_list = self._run_esearch(
                    client=client,
                    query=query,
                    max_results=max_results,
                )
                for item in id_list:
                    if item not in collected_ids:
                        collected_ids.append(item)
                if len(collected_ids) >= max_results:
                    break
        return collected_ids[:max_results]

    def _run_esearch(
        self,
        client: httpx.Client,
        query: str,
        max_results: int,
    ) -> list[str]:
        time.sleep(NCBI_RATE_LIMIT_DELAY)
        params = {
            "db": "pubmed",
            "retmode": "json",
            "retmax": max(1, max_results),
            "tool": self._tool,
            "term": query,
        }
        if self._email:
            params["email"] = self._email

        response = client.get(f"{self._base}/esearch.fcgi", params=params)
        response.raise_for_status()
        payload = response.json()
        id_list = payload.get("esearchresult", {}).get("idlist", [])
        if not isinstance(id_list, list):
            return []
        return [item for item in id_list if isinstance(item, str)]

    @staticmethod
    def _build_token_query(cleaned_title: str) -> str:
        words = re.findall(r"[a-zA-Z0-9-]{3,}", cleaned_title.lower())
        stop_words = {
            "the",
            "and",
            "for",
            "with",
            "from",
            "into",
            "that",
            "this",
            "study",
            "effect",
            "effects",
        }
        tokens: list[str] = []
        for word in words:
            if word in stop_words:
                continue
            if word not in tokens:
                tokens.append(word)
            if len(tokens) >= 8:
                break
        if not tokens:
            return ""
        return " AND ".join(f"{token}[Title/Abstract]" for token in tokens)

    def fetch_related_pmids(
        self,
        pmid: str,
        max_results: int = 10,
    ) -> list[str]:
        """
        Fetch related/similar articles from PubMed (references + cited-in).
        Returns up to max_results PMIDs.
        """
        params = {
            "dbfrom": "pubmed",
            "db": "pubmed",
            "id": pmid,
            "retmode": "json",
            "tool": self._tool,
        }
        if self._email:
            params["email"] = self._email
        try:
            time.sleep(NCBI_RATE_LIMIT_DELAY)
            with httpx.Client(timeout=25.0) as client:
                response = client.get(
                    f"{self._base}/elink.fcgi",
                    params=params,
                )
                response.raise_for_status()
                data = response.json()
        except Exception:
            return []

        collected: list[str] = []
        pmid_str = str(pmid)
        linksets = data.get("linksets", [])
        for linkset in linksets:
            for linksetdb in linkset.get("linksetdbs", []):
                if linksetdb.get("dbto") != "pubmed":
                    continue
                linkname = linksetdb.get("linkname", "")
                links = linksetdb.get("links", [])
                for item in links:
                    sid = str(item) if isinstance(item, (str, int)) else None
                    if sid and sid != pmid_str and sid not in collected:
                        collected.append(sid)
                        if len(collected) >= max_results:
                            return collected[:max_results]

        return collected[:max_results]

    @staticmethod
    def _extract_year(pubdate: str) -> int | None:
        match = re.search(r"\b(19|20)\d{2}\b", pubdate)
        if not match:
            return None
        return int(match.group(0))

    @staticmethod
    def _extract_pmc_url(elink_response: dict) -> str | None:
        linksets = elink_response.get("linksets", [])
        for linkset in linksets:
            for db in linkset.get("linksetdbs", []):
                if db.get("dbto") != "pmc":
                    continue
                links = db.get("links", [])
                if not links:
                    continue
                pmc_id = links[0]
                return f"https://pmc.ncbi.nlm.nih.gov/articles/PMC{pmc_id}/"
        return None
