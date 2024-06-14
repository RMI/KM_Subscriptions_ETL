import requests
from bs4 import BeautifulSoup

# Define the URL of the website
url = "https://www.aljazeera.com/"

# Send a GET request to the website
response = requests.get(url)

# Parse the HTML content using BeautifulSoup
soup = BeautifulSoup(response.content, "html.parser")

# Find all the links on the page
links = soup.find_all("a")

print("Number of links found:", len(links))

# Iterate over each link
for link in links:
    # Get the href attribute of the link
    href = link.get("href")
    
    # If href is not None and is a relative URL (i.e., a subpage)
    if href and href.startswith("/"):
        # Send a GET request to the subpage
        subpage_response = requests.get(url + href)
        subpage_response = requests.get(url)

        # Parse the HTML content of the subpage
        subpage_soup = BeautifulSoup(subpage_response.content, "html.parser")

        # Find all the article elements on the page
        articles = soup.find_all("card_gallery")

        # Extract the summary information from each article
        for article in articles:
            title = article.find("article-card__title").text.strip()
            summary = article.find("article-card__excerpt").text.strip()
            print("Title:", title)
            print("Summary:", summary)
            print("--------------------")