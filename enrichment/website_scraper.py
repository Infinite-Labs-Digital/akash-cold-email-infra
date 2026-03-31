import logging
import requests
from urllib.parse import urljoin, urlparse
from html.parser import HTMLParser
from config import CONTACT_PATHS, WEBSITE_SCRAPE_MAX_SUBPAGES, WEBSITE_SCRAPE_TIMEOUT

logger = logging.getLogger(__name__)


class TextExtractor(HTMLParser):
    """Extract visible text from HTML, stripping tags."""
    def __init__(self):
        super().__init__()
        self.text_parts = []
        self._skip = False
        self._skip_tags = {"script", "style", "noscript", "header", "footer", "nav"}

    def handle_starttag(self, tag, attrs):
        if tag in self._skip_tags:
            self._skip = True

    def handle_endtag(self, tag):
        if tag in self._skip_tags:
            self._skip = False

    def handle_data(self, data):
        if not self._skip:
            text = data.strip()
            if text:
                self.text_parts.append(text)

    def get_text(self):
        return " ".join(self.text_parts)


def scrape_website(website: str) -> dict:
    """Scrape homepage + up to 20 subpages. Returns {pages: [{url, text}], emails: []}."""
    if not website:
        return {"pages": [], "emails": []}

    base_url = website if "://" in website else f"https://{website}"
    base_domain = urlparse(base_url).netloc.replace("www.", "")

    pages = []
    emails = set()

    # Scrape homepage first
    homepage_text, homepage_links = _fetch_page(base_url)
    if homepage_text:
        pages.append({"url": base_url, "text": homepage_text})
        emails.update(_extract_emails(homepage_text, base_domain))

    # Find subpage URLs: contact paths + links from homepage
    subpage_urls = set()
    for path in CONTACT_PATHS:
        subpage_urls.add(urljoin(base_url, path))
    for link in homepage_links:
        if _is_same_domain(link, base_domain):
            subpage_urls.add(link)

    # Scrape subpages (up to max)
    scraped = 0
    for url in subpage_urls:
        if scraped >= WEBSITE_SCRAPE_MAX_SUBPAGES:
            break
        if url == base_url:
            continue
        text, _ = _fetch_page(url)
        if text and len(text) > 50:
            pages.append({"url": url, "text": text})
            emails.update(_extract_emails(text, base_domain))
            scraped += 1

    logger.debug(f"Scraped {len(pages)} pages from {base_url}, found {len(emails)} emails")
    return {"pages": pages, "emails": list(emails)}


def _fetch_page(url: str) -> tuple:
    """Fetch a single page. Returns (text, links)."""
    try:
        resp = requests.get(url, timeout=WEBSITE_SCRAPE_TIMEOUT,
                           headers={"User-Agent": "Mozilla/5.0"})
        if resp.status_code != 200:
            return "", []

        extractor = TextExtractor()
        extractor.feed(resp.text)
        text = extractor.get_text()

        # Extract links
        links = []
        import re
        for match in re.finditer(r'href=["\']([^"\']+)["\']', resp.text):
            href = match.group(1)
            if href.startswith("http"):
                links.append(href)
            elif href.startswith("/"):
                links.append(urljoin(url, href))

        return text, links
    except Exception as e:
        logger.debug(f"Failed to fetch {url}: {e}")
        return "", []


def _is_same_domain(url: str, base_domain: str) -> bool:
    parsed = urlparse(url)
    return base_domain in (parsed.netloc or "").replace("www.", "")


def _extract_emails(text: str, base_domain: str) -> set:
    """Extract email addresses from text, filtering junk."""
    import re
    from config import JUNK_EMAIL_PATTERNS
    pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
    found = set(re.findall(pattern, text))
    return {e.lower() for e in found
            if not any(junk in e.lower() for junk in JUNK_EMAIL_PATTERNS)}
