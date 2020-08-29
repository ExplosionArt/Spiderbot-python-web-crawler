# Crawler script
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pymongo import MongoClient
from bs4 import BeautifulSoup
from time import sleep
from cfg import config
from utils import save_file, mark_link_crawled, get_random_file_name

# First try the simple method, without OOP

def SpiderBot(links_collection):
    # Do this process forever
    while True:
        # Get links for database
        links = links_collection.find()
        #print('I have fetched links from the database')

        #num_links = links_collection.count_documents({})
        #print('Number of links I found: ' + str(num_links))

        # If there are links that are not crawled yet, which are within max_links
        if links_collection.count_documents({"isCrawled" : False}) == 0:
            # Print max limit reached
            print('Maximum limit reached')
            # Ignore everything and continue
            continue

        # For each link
        def Execute(link):
            # Check If link is crawled in the last 24 hours
            previous_day = datetime.now() - timedelta(hours = 24)
            if link['isCrawled'] and link['lastCrawlDt'] >= previous_day:
                # Ignore this link and continue
                #continue
                return

            # If links is not crawled at all or it is not crawled in last 24 hours then
            else:
                #print('Not crawled recently')
                # Do the web request
                url = link['link']
                print('I will now do a web request to', end = " ")
                print(url)
                try:
                    r = requests.get(url)
                except Exception:
                    return

                # Check the status code
                # Check If status code is not 200
                if r.status_code != 200:
                    # Mark link as isCrawled = true and continue with next link
                    mark_link_crawled(links_collection, url, datetime.now(), r.status_code, None, None, None)
                    #continue
                    return

                # Find out the content type of response
                content_type = r.headers['content-type']

                # If content type is html
                if 'text/html' in content_type:
                    # Extract <a href = ""> links
                    parsed_html = BeautifulSoup(r.content, "html.parser")
                    file_path = "response_files\\" + get_random_file_name(content_type)

                    # Save all valid links to database
                    for a in parsed_html.find_all('a', href = True):
                        # First check if database size is already at max_links or not
                        if links_collection.count_documents({}) >= config['max_links']:
                            break

                        if a['href'] == "" or a['href'][0] == '//':
                            continue

                        if a['href'][0] == '/':
                            a['href'] = requests.compat.urljoin(link['sourceLink'], a['href'])
                            
                        if not (requests.compat.urlparse(a['href']).scheme and requests.compat.urlparse(a['href']).netloc):
                            continue

                        # Add entry to database only if link is unique
                        if links_collection.count_documents({"link" : a['href']}) == 0:
                            links_collection.insert_one({
                                "link" : a['href'], 
                                "sourceLink" : url, 
                                "isCrawled" : False, 
                                "lastCrawlDt" : datetime(2020, 1, 1, 0, 0), 
                                "responseStatus" : None, 
                                "contentType" : None, 
                                "contentLength" : None, 
                                "filePath" : None, 
                                "createdAt" : datetime.now()
                            })
                
                    # Save the file on disk
                    save_file(r.content, file_path)

                    # Mark links as isCrawled = True and continue with next link
                    mark_link_crawled(links_collection, url, datetime.now(), r.status_code, content_type, len(r.content), file_path)

                # If content type is not html
                else:
                    # Based on content type, create a file name
                    file_path = "response_files\\" + get_random_file_name(content_type)

                    # Save the file on disk
                    save_file(r.content, file_path)

                    # Mark link as isCrawled = True and continue with next link
                    mark_link_crawled(links_collection, url, datetime.now(), r.status_code, content_type, len(r.content), file_path)
        
        with ThreadPoolExecutor(max_workers = config['max_threads']) as executor:
            executor.map(Execute, links)
        print('One Cycle Complete. Sleep for 5 seconds')
        # Sleep for 5 seconds
        sleep(config['sleep_time'])

if __name__ == "__main__":
    # Database configurations and connection establishment
    try:
        client = MongoClient()
        db = client[config['db']]
        links_collection = db[config['collection']]

    except Exception as inst:
        print("MongoDB connection Error", inst.args)

    # Check if root URL exists in database
    if links_collection.find_one({"link" : config['root_url']}) is None:
        links_collection.insert_one({
            "link" : config['root_url'],
            "sourceLink" : "",
            "isCrawled" : False,
            "lastCrawlDt" : datetime(2020, 1, 1, 0, 0),
            "responseStatus" : 0,
            "contentType" : "",
            "contentLength" : 0,
            "filePath" : "",
            "createdAt" : datetime.now()
        })
    
    SpiderBot(links_collection)
