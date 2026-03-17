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


def clean_text(text):
    if not text:
        return ""
    return re.sub(r'^[^\w\+]+', '', text.strip())


def extract_domain(url):
    try:
        return urlparse(url).netloc.lower().replace("www.", "")
    except:
        return ""


def scroll_until_end(page):
    previous = 0
    stable = 0

    while True:
        page.evaluate("""
        const feed = document.querySelector('div[role="feed"]');
        if (feed) feed.scrollBy(0, 6000);
        """)

        page.wait_for_timeout(1200)

        count = page.locator("div.Nv2PK").count()

        if count == previous:
            stable += 1
        else:
            stable = 0

        previous = count

        if stable >= 5:
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

    return list(set(links))


def extract_email_from_html(html):
    emails = re.findall(EMAIL_REGEX, html)
    for email in emails:
        email = email.lower()
        if not any(x in email for x in ["png", "jpg", "jpeg", "svg", "example"]):
            return email
    return ""


def scrape_email(context, website):
    if not website:
        return ""

    page = context.new_page()

    try:
        page.set_default_timeout(8000)

        page.goto(website)
        html = page.content()
        email = extract_email_from_html(html)

        if email:
            page.close()
            return email

        # only 1 extra page
        try:
            page.goto(urljoin(website, "/contact"))
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
        page.set_default_timeout(10000)
        page.goto(url, wait_until="domcontentloaded")
    except:
        page.close()
        return None

    try:
        name = clean_text(page.locator("h1").inner_text())
    except:
        name = ""

    try:
        rating_block = page.locator("div.F7nice").inner_text()
        rating = re.search(r'([0-9.]+)', rating_block)
        reviews = re.search(r'\(([\d,]+)\)', rating_block)

        rating = rating.group(1) if rating else ""
        reviews = reviews.group(1).replace(",", "") if reviews else ""
    except:
        rating = ""
        reviews = ""

    try:
        address = clean_text(page.locator('button[data-item-id="address"]').inner_text())
    except:
        address = ""

    try:
        phone = clean_text(page.locator('button[data-item-id^="phone"]').inner_text())
    except:
        phone = ""

    try:
        website = page.locator('a[data-item-id="authority"]').get_attribute("href")
    except:
        website = ""

    page.close()

    email = scrape_email(context, website)

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
    all_results = []

    with sync_playwright() as p:

        browser = p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"]
        )

        context = browser.new_context()

        for query in SEARCH_QUERIES:

            print("\nSearching:", query)

            page = context.new_page()
            page.goto(f"https://www.google.com/maps/search/{query.replace(' ','+')}")
            page.wait_for_selector('div[role="feed"]')

            scroll_until_end(page)
            links = collect_links(page)

            if TEST_LIMIT:
                links = links[:TEST_LIMIT]

            print(f"Processing {len(links)} companies...")

            results = []

            for link in links:
                result = scrape_place(context, link)

                if result:
                    print("Collected:", result["company_name"])

                    domain = extract_domain(result["website"])
                    if domain and domain in seen_domains:
                        continue

                    seen_domains.add(domain)
                    results.append(result)
                    all_results.append(result)

            pd.DataFrame(results).to_csv(f"maps_{query.replace(' ','_')}.csv", index=False)

        browser.close()

    pd.DataFrame(all_results).to_csv("maps_ALL_QUERIES.csv", index=False)

    print("\nDONE. Total companies:", len(all_results))


if __name__ == "__main__":
    main()