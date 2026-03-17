import asyncio
from playwright.async_api import async_playwright
import pandas as pd
import re
from urllib.parse import urlparse

SEARCH_QUERIES = [
    "seo agency Sofia",
    "marketing agency Sofia"
]

TEST_LIMIT = 20
CONCURRENT_PAGES = 5

EMAIL_REGEX = r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}"


def extract_domain(url):
    try:
        return urlparse(url).netloc.lower().replace("www.", "")
    except:
        return ""


def extract_email(html):
    emails = re.findall(EMAIL_REGEX, html)
    return emails[0] if emails else ""


async def block_resources(route):
    if route.request.resource_type in ["image", "stylesheet", "font"]:
        await route.abort()
    else:
        await route.continue_()


async def scroll(page):
    for _ in range(6):
        await page.mouse.wheel(0, 5000)
        await page.wait_for_timeout(800)


async def collect_links(page):
    cards = await page.locator("div.Nv2PK").all()
    links = []

    for card in cards:
        try:
            link = await card.locator("a").first.get_attribute("href")
            if link and "/place/" in link:
                links.append(link)
        except:
            pass

    return list(set(links))


async def scrape_place(context, url, semaphore):
    async with semaphore:
        page = await context.new_page()

        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=15000)

            name = await page.locator("h1").inner_text(timeout=5000)

            try:
                website = await page.locator('a[data-item-id="authority"]').get_attribute("href")
            except:
                website = ""

            await page.close()

            return {
                "name": name,
                "website": website,
                "maps_url": url
            }

        except:
            await page.close()
            return None


async def scrape_email(context, item, semaphore):
    if not item["website"]:
        item["email"] = ""
        return item

    async with semaphore:
        page = await context.new_page()

        try:
            await page.goto(item["website"], wait_until="domcontentloaded", timeout=10000)
            html = await page.content()
            item["email"] = extract_email(html)
        except:
            item["email"] = ""

        await page.close()
        return item


async def scrape_query(context, query):
    page = await context.new_page()

    url = f"https://www.google.com/maps/search/{query.replace(' ','+')}"
    await page.goto(url, wait_until="domcontentloaded", timeout=60000)

    await page.wait_for_timeout(3000)
    await scroll(page)

    links = await collect_links(page)

    if TEST_LIMIT:
        links = links[:TEST_LIMIT]

    print(f"{query} → {len(links)} places")

    semaphore = asyncio.Semaphore(CONCURRENT_PAGES)

    # 🔥 PARALLEL MAPS SCRAPING
    tasks = [scrape_place(context, link, semaphore) for link in links]
    results = await asyncio.gather(*tasks)

    results = [r for r in results if r]

    await page.close()
    return results


async def enrich_emails(context, results):
    semaphore = asyncio.Semaphore(CONCURRENT_PAGES)

    tasks = [scrape_email(context, r, semaphore) for r in results]
    results = await asyncio.gather(*tasks)

    return results


async def main():
    all_results = []
    seen_domains = set()

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"]
        )

        context = await browser.new_context()

        # 🚀 BLOCK HEAVY FILES
        await context.route("**/*", block_resources)

        for query in SEARCH_QUERIES:
            print("\nQuery:", query)

            results = await scrape_query(context, query)
            results = await enrich_emails(context, results)

            filtered = []

            for r in results:
                domain = extract_domain(r["website"])
                if domain and domain in seen_domains:
                    continue
                seen_domains.add(domain)
                filtered.append(r)

            all_results.extend(filtered)

            pd.DataFrame(filtered).to_csv(f"{query.replace(' ','_')}.csv", index=False)

        await browser.close()

    pd.DataFrame(all_results).to_csv("ALL_RESULTS.csv", index=False)

    print("\nDONE:", len(all_results))


if __name__ == "__main__":
    asyncio.run(main())