from playwright.sync_api import sync_playwright
import pandas as pd
import re
from urllib.parse import urljoin, urlparse

cities = [
    "Plovdiv","Varna","Burgas","Ruse","Stara Zagora",
    "Veliko Tarnovo","Blagoevgrad"
]

services = [
    "seo agency",
    "marketing agency",
    "advertising agency",
    "web design agency"
]

SEARCH_QUERIES = [f"{service} {city}" for city in cities for service in services]

TEST_LIMIT = 5

EMAIL_REGEX = r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}"

CONTACT_PATHS = [
    "/contact",
    "/contact-us",
    "/about",
    "/imprint"
]


def clean_text(text):
    if not text:
        return ""
    text = text.strip()
    text = re.sub(r'^[^\w\+]+', '', text)
    return text


def extract_domain(url):
    try:
        domain = urlparse(url).netloc.lower()
        return domain.replace("www.", "")
    except:
        return ""


def scroll_until_end(page):
    print("Scrolling results...")
    previous = 0
    stable = 0

    while True:
        page.evaluate("""
        const feed = document.querySelector('div[role="feed"]');
        if (feed) {
            feed.scrollBy(0, 6000);
        }
        """)

        page.wait_for_timeout(3000)
        count = page.locator("div.Nv2PK").count()
        print("Listings loaded:", count)

        if count == previous:
            stable += 1
        else:
            stable = 0

        previous = count

        if stable >= 12:
            print("End of results reached")
            break


def collect_links(page):
    cards = page.locator("div.Nv2PK")
    links = []

    for i in range(cards.count()):
        try:
            link = cards.nth(i).locator("a").first.get_attribute("href")
            if link and "/place/" in link:
                links.append(link)
        except:
            pass

    links = list(set(links))
    print("Unique places:", len(links))
    return links


def extract_email_from_html(html):
    emails = re.findall(EMAIL_REGEX, html)
    filtered = []

    for email in emails:
        email = email.lower()
        if any(x in email for x in ["png", "jpg", "jpeg", "svg", "example"]):
            continue
        filtered.append(email)

    return filtered[0] if filtered else ""


def scrape_emails_from_site(context, website):
    if not website:
        return ""

    page = context.new_page()

    try:
        page.goto(website, timeout=20000)
        page.wait_for_timeout(2000)

        html = page.content()
        email = extract_email_from_html(html)

        if email:
            page.close()
            return email

        for path in CONTACT_PATHS:
            try:
                url = urljoin(website, path)
                page.goto(url, timeout=15000)
                page.wait_for_timeout(2000)

                html = page.content()
                email = extract_email_from_html(html)

                if email:
                    page.close()
                    return email
            except:
                pass

    except:
        pass

    page.close()
    return ""


def scrape_place(context, url):
    page = context.new_page()

    try:
        page.goto(url, wait_until="domcontentloaded", timeout=30000)
        page.wait_for_selector("h1", timeout=10000)
    except:
        page.close()
        return None

    try:
        name = clean_text(page.locator("h1").inner_text())
    except:
        name = ""

    try:
        rating_block = page.locator("div.F7nice").inner_text()
        rating_match = re.search(r'([0-9.]+)', rating_block)
        reviews_match = re.search(r'\(([\d,]+)\)', rating_block)

        rating = rating_match.group(1) if rating_match else ""
        reviews = reviews_match.group(1).replace(",", "") if reviews_match else ""
    except:
        rating = ""
        reviews = ""

    try:
        address = clean_text(
            page.locator('button[data-item-id="address"]').inner_text()
        )
    except:
        address = ""

    try:
        phone = clean_text(
            page.locator('button[data-item-id^="phone"]').inner_text()
        )
    except:
        phone = ""

    try:
        website = page.locator(
            'a[data-item-id="authority"]'
        ).get_attribute("href")
    except:
        website = ""

    page.close()

    email = ""
    if website:
        print("Scanning website:", website)
        email = scrape_emails_from_site(context, website)

    return {
        "company_name": name,
        "rating": rating,
        "reviews": reviews,
        "phone": phone,
        "location": address,
        "website": website,
        "email": email,
        "maps_url": url
    }


def main():
    seen_domains = set()
    seen_fallback = set()
    all_results = []

    with sync_playwright() as p:

        browser = p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"]
        )

        context = browser.new_context()

        for query in SEARCH_QUERIES:
            page = context.new_page()

            search = query.replace(" ", "+")
            url = f"https://www.google.com/maps/search/{search}"

            print("\nSearching:", query)

            page.goto(url)
            page.wait_for_selector('div[role="feed"]')

            scroll_until_end(page)
            links = collect_links(page)

            print("Scraping companies...")
            query_results = []

            for link in links:
                if TEST_LIMIT and len(query_results) >= TEST_LIMIT:
                    break

                result = scrape_place(context, link)

                if result:
                    website = result["website"]
                    domain = extract_domain(website)

                    if domain:
                        if domain in seen_domains:
                            continue
                        seen_domains.add(domain)
                    else:
                        fallback_key = result["company_name"] + result["location"]
                        if fallback_key in seen_fallback:
                            continue
                        seen_fallback.add(fallback_key)

                    query_results.append(result)
                    all_results.append(result)

                    print("Collected:", result["company_name"])

            filename = f"maps_{query.replace(' ','_')}.csv"
            pd.DataFrame(query_results).to_csv(filename, index=False)
            print("Saved:", filename)

        context.close()
        browser.close()

    df_all = pd.DataFrame(all_results)
    df_all.to_csv("maps_ALL_QUERIES.csv", index=False)

    print("\nTotal unique companies scraped:", len(df_all))


if __name__ == "__main__":
    main()