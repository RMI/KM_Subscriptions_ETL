import json
import requests
from bs4 import BeautifulSoup
 
def getNewsData():
    headers = {
        "User-Agent":
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.54 Safari/537.36"
    }
    response = requests.get(
        "https://www.google.com/search?q=us+stock+markets&gl=us&tbm=nws&num=100", headers=headers
    )
    soup = BeautifulSoup(response.content, "html.parser")
    news_results = []
 
    for el in soup.select("div.SoaBEf"):
        news_results.append(
            {
                "link": el.find("a")["href"],
                "title": el.select_one("div.MBeuO").get_text(),
                "snippet": el.select_one(".GI74Re").get_text(),
                "date": el.select_one(".LfVVr").get_text(),
                "source": el.select_one(".NUnG9d span").get_text()
            }
        )
  
    print(json.dumps(news_results, indent=2))
  
getNewsData()