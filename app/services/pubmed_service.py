import re

import httpx

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
            summary_response = client.get(
                f"{self._base}/esummary.fcgi",
                params=params,
            )
            summary_response.raise_for_status()
            summary_data = summary_response.json()

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

        result = summary_data["result"][pmid]
        authors = [author["name"] for author in result.get("authors", [])]
        year = self._extract_year(result.get("pubdate", ""))
        full_text_url = self._extract_pmc_url(pmc_data)

        return ResearchItem(
            title=result.get("title", "").strip(),
            authors=authors,
            year=year,
            pmid=pmid,
            pmid_url=f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/",
            full_text_url=full_text_url,
        )

    def search_pmids_by_title(
        self,
        title: str,
        max_results: int = 5,
    ) -> list[str]:
        cleaned_title = title.strip()
        if not cleaned_title:
            return []

        query_candidates = [
            f"\"{cleaned_title}\"[Title]",
            f"\"{cleaned_title}\"[Title/Abstract]",
        ]
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
