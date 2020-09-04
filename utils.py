import random
import string
import requests
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from cfg import config


def previous_day():
    return datetime.now() - timedelta(hours = 24)

def extract_links(links_collection, r, link, url):
    parsed_html = BeautifulSoup(r.content, "html.parser")

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

def save_file(response_bytes, filepath):
    with open(filepath, 'wb') as f:
        f.write(response_bytes)

def mark_link_crawled(links_collection, link, lastCrawlDt, status_code, content_type, content_length, file_path):
    links_collection.update_one({"link" : link},{"$set":{
                "isCrawled" : True,
                "lastCrawlDt" : lastCrawlDt,
                "responseStatus" : status_code,
                "contentType" : content_type,
                "contentLength": content_length,
                "filePath" : file_path
                }})

def get_random_file_name(content_type):
    # Generation of 24 - bit random alphanumeric string
    letters_and_digits = string.ascii_letters + string.digits
    rndstring = ''.join((random.choice(letters_and_digits) for i in range(24)))

    # Append appropriate extension for the content of html content
    extension = ''

    if 'image/png' in content_type:
        extension = '.png'
    elif 'audio/aac' in content_type:
        extension = '.aac'
    elif 'video/x-msvideo' in content_type:
        extension = '.avi'
    elif 'application/octet-stream' in content_type:
        extension = '.bin'
    elif 'image/bmp' in content_type:
        extension = '.bmp'
    elif 'text/css' in content_type:
        extension = '.css'
    elif 'text/csv' in content_type:
        extension = '.csv'
    elif 'application/msword' in content_type:
        extension = '.doc'
    elif 'application/vnd.openxmlformats-officedocument.wordprocessingml.document' in content_type:
        extension = '.docx'
    elif 'image/gif' in content_type:
        extension = '.gif'
    elif 'text/html' in content_type:
        extension = '.html'
    elif 'image/vnd.microsoft.icon' in content_type:
        extension = '.ico'
    elif 'image/jpeg' in content_type:
        extension = '.jpg'
    elif 'text/javascript' in content_type:
        extension = '.js'
    elif 'application/json' in content_type:
        extension = '.json'
    elif 'application/ld+json' in content_type:
        extension = '.jsonld'
    elif 'audio/midi' in content_type or 'audio/x-midi' in content_type:
        extension = '.midi'
    elif 'audio/mpeg' in content_type:
        extension = '.mp3'
    elif 'video/mpeg' in content_type:
        extension = '.mpeg'
    elif 'audio/ogg' in content_type:
        extension = '.oga'
    elif 'video/ogg' in content_type:
        extension = '.ogv'
    elif 'application/pdf' in content_type:
        extension = '.pdf'
    elif 'application/x-httpd-php' in content_type:
        extension = '.php'
    elif 'application/vnd.ms-powerpoint' in content_type:
        extension = '.ppt'
    elif 'application/vnd.openxmlformats-officedocument.presentationml.presentation' in content_type:
        extension = '.pptx'
    elif 'application/vnd.rar' in content_type:
        extension = '.rar'
    elif 'application/x-7z-compressed' in content_type:
        extension = '.7z'
    elif 'application/zip' in content_type:
        extension = '.zip'
    elif 'application/vnd.ms-excel' in content_type:
        extension = '.xls'
    elif 'application/xml' in content_type or 'text/xml' in content_type:
        extension = '.xml'
    elif 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' in content_type:
        extension = '.xlsx'
    elif 'audio/webm' in content_type:
        extension = '.weba'
    elif 'video/webm' in content_type:
        extension = '.webm'
    elif 'audio/wav' in content_type:
        extension = '.wav'
    elif 'text/plain' in content_type:
        extension = '.txt'
    elif 'image/tiff' in content_type:
        extension = '.tiff'
    elif 'image/svg+xml' in content_type:
        extension = '.svg'
    elif 'image/webp' in content_type:
        extension = '.webp'

    return rndstring + extension