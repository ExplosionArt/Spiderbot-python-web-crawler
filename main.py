# Crawler script
import requests
from os import path, makedirs
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from pymongo import MongoClient
from bs4 import BeautifulSoup
from time import sleep
from cfg import config
from utils import save_file, mark_link_crawled, get_random_file_name

def previous_day():
    return datetime.now() - timedelta(hours = 24)

def SpiderBot(links_collection):
    # Do this process forever
    while True:
        # Get links for database
        links = links_collection.find()
        #print('I have fetched links from the database')

        # Check if max limit reached
        if links_collection.count_documents({}) >= config['max_links']:
            # Print max limit reached
            print('Maximum limit reached')
            # Ignore everything and continue
            # continue
            # Database limit is reached. However, all links may not be crawled yet.
            

        if links_collection.count_documents({"isCrawled" : False}) == 0 and links_collection.count_documents({"lastCrawlDt" : { "$lt": previous_day()}}) == 0:
            # All links in DB crawled. Ignore everything and continue
            print('All Links crawled')
            continue

        # Define a function for ThreadPoolExecutor For each link
        def Execute(link):
            # Check If link is crawled in the last 24 hours
            if link['isCrawled'] and link['lastCrawlDt'] >= previous_day():
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

                except requests.exceptions.HTTPError:
                    # Invalid HTTP Response
                    print("HTTP exception for " + url)
                    mark_link_crawled(links_collection, url, datetime.now(), None, None, None, None)

                except requests.exceptions.SSLError:
                    # Some problematic link(like telegram app link) has SSL Errors, which should not be encounterd again for today
                    print("SSL exception for " + url)
                    return

                except requests.exceptions.Timeout:
                    print("Timeout exception for " + url)
                    # Do nothing and simply try to get it again in next cycle
                    return

                except requests.exceptions.ConnectionError:
                    print("Connection Error for " + url)
                    # Connection Error, Do nothing and try again in next cycle
                    return

                except requests.exceptions.RequestException:
                    print("Request Exception for " + url)
                    # Ambiguous Exception
                    return
                
                except Exception:
                    print("Exception for " + url)
                    # Any other exception which is not handled, mark the link as crawled and move on
                    mark_link_crawled(links_collection, url, datetime.now(), None, None, None, None)
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

                    # Check if the current url was already crawled and 24 hours have passed
                    if link['isCrawled'] and link['lastCrawlDt'] < previous_day():
                        # Get the file path from database entry, and overwrite the same file
                        file_path = link['filePath']
                    else:
                        # This file was not crawled previously, so get a new name
                        file_path = config['file_dir'] + get_random_file_name(content_type)

                    # Save all valid links to database
                    for a in parsed_html.find_all('a', href = True):

                        # First check if database size is already at max_links
                        if links_collection.count_documents({}) >= config['max_links']:
                            # Do not add any new links to database, simply break
                            break

                        # Check for relative links
                        if a['href'] == "" or a['href'][0] == '//':
                            continue

                        if a['href'][0] == '/':
                            a['href'] = requests.compat.urljoin(link['sourceLink'], a['href'])
                            
                        if not (requests.compat.urlparse(a['href']).scheme and requests.compat.urlparse(a['href']).netloc):
                            continue

                        # Add new entry to database only if link is unique
                        if links_collection.count_documents({"link" : a['href']}) == 0:
                            links_collection.insert_one({
                                "link" : a['href'], 
                                "sourceLink" : url, 
                                "isCrawled" : False, 
                                "lastCrawlDt" : None, 
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
                    file_path = config['file_dir'] + get_random_file_name(content_type)

                    # Save the file on disk
                    save_file(r.content, file_path)

                    # Mark link as isCrawled = True and continue with next link
                    mark_link_crawled(links_collection, url, datetime.now(), r.status_code, content_type, len(r.content), file_path)
        
        # Multithreading with 'max_threads'
        with ThreadPoolExecutor(max_workers = config['max_threads']) as executor:
            executor.map(Execute, links)
        
        # Print that one cycle is complete
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

    # Check if the response-files directory is already present or not, else create it
    if not path.exists(config['file_dir']):
        makedirs(config['file_dir'])

    # Check if root URL exists in database
    if links_collection.find_one({"link" : config['root_url']}) is None:
        links_collection.insert_one({
            "link" : config['root_url'],
            "sourceLink" : "",
            "isCrawled" : False,
            "lastCrawlDt" : None,
            "responseStatus" : 0,
            "contentType" : "",
            "contentLength" : 0,
            "filePath" : "",
            "createdAt" : datetime.now()
        })
    
    # Call the SpiderBot
    SpiderBot(links_collection)
