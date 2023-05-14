import time
from datetime import datetime

import polars as pl
from bs4 import BeautifulSoup
from playwright.sync_api import Playwright, sync_playwright
from tqdm import tqdm, trange

now = datetime.now()


def run(playwright: Playwright) -> str:
    browser = playwright.chromium.launch(headless=False, slow_mo=1000)
    context = browser.new_context()
    page = context.new_page()
    page.goto("https://www.forbes.com/consent/?toURL=https://www.forbes.com/real-time-billionaires/")
    page.get_by_role("button", name="Accept All").click()
    page.get_by_text("THE REAL-TIME BILLIONAIRES LIST2023 ListRequest SpreadsheetReprintsFilter list b").click()
    for _ in trange(120):
        page.mouse.wheel(0, 5000)
        time.sleep(5)
    time.sleep(5)
    content = page.content()
    # ---------------------
    context.close()
    browser.close()
    return content


with sync_playwright() as playwright:
    forbes_html = run(playwright)

soup = BeautifulSoup(forbes_html, "html.parser")
table = soup.find("table", class_="ng-scope ng-table").find("tbody").find_all("tr", class_="base ng-scope")

json_list = []
for row in tqdm(table):
    rank = getattr(row.find("td", class_="rank").find("div", class_="ng-scope").find("span", class_="ng-binding"),
                   "text", None)
    name = getattr(row.find("td", class_="name").find("div", class_="ng-scope").find("h3"), "text", None)
    networth = getattr(
        row.find("td", class_="Net Worth").find("div", class_="ng-scope").find("span", class_="ng-binding"), "text",
        None)
    age = getattr(row.find("td", class_="age").find("div", class_="ng-scope").find("span", class_="ng-binding"), "text",
                  None)
    if age == "":
        age = "0"
    source = getattr(row.find("td", class_="source").find("div", class_="ng-scope").find("span", class_="ng-binding"),
                     "text", None)
    country = getattr(
        row.find("td", class_="Country/Territory").find("div", class_="ng-scope").find("span", class_="ng-binding"),
        "text", None)
    row_dict = {"Rank": rank, "Name": name, "NetWorth": networth, "Age": age, "Source": source, "Country": country}
    json_list.append(row_dict)

if len(table) == len(json_list):
    pass
else:
    pass

pl.from_dicts(json_list) \
    .with_column(pl.col("Rank").cast(pl.Int64)) \
    .with_column(
    pl.col("Name").apply(lambda x: x.split(" ")[1] if len(x.split(" ")) > 1 else x.split(" ")[0]).alias("LastName")) \
    .with_column(pl.col("Age").cast(pl.Int64)) \
    .with_column(pl.col("NetWorth").str.strip("$B ").cast(pl.Float64).apply(lambda x: x * 10 ** 9).cast(pl.Int64)) \
    .with_column(pl.lit(now).alias("Timestamp")) \
    .write_ipc(f"data/forbes/forbes-{now.strftime('%Y-%m-%d-%H-%M-%S')}.feather", compression="lz4")