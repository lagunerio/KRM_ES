import amazon.api
import bottlenose.api
import codecs
import sys
import requests
import json
import time
import urllib2
import elasticsearch
import os
from socket import *
import paramiko
from lxml import etree
from os import fork

reload(sys)
sys.setdefaultencoding('utf-8')

#amazon api access data
AMAZON_ACCESS_KEY = 'AKIAJASDKNLGA2OPJLTQ'
AMAZON_SECRET_KEY = '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'
AMAZON_ASSOC_TAG = 'mymi-21'

RESULT_FILE = '/tmp/mymik/results/AMZ_DE.csv'

#elasticsearch access data
ESS = "http://localhost:9200"
ESS_INDEX = "articles_price"
ESS_INDEX_TYPE = "product"
ESS_FUNC_UPDATE = "_update"
ESS_FUNC_SEARCH = "_search"

#Get amazon articles from elasticsearch
def get_spn_list():
    try:
        es_client = elasticsearch.Elasticsearch(ESS)

        #get articles that have 'AMZ.de' at artnumber
        #to get more than 10000 articles, use scroll
        docs = es_client.search(index = ESS_INDEX,
                                doc_type = ESS_INDEX_TYPE,
                                body = { 'query' : { 'match' : { 'artnumber':'AMZ_DE' } } },
                                scroll = '1m',
                                size = 5000
                            )
        num_docs = docs['hits']['total']
        scroll_id = docs['_scroll_id']
        spn_list = []

        #add article's ordernumber and variantid at list for first time
        for article in docs['hits']['hits']:
            spn_list.append({"spn":article['_source']["ordernumber"], "variantid":str(article['_source']['variantid'])})
        while num_docs > 0:
            #add article's ordernumber and variantid
            #it works to the end of scroll
            docs = es_client.scroll(scroll_id = scroll_id, scroll='1m')
            num_docs = len(docs['hits']['hits'])
            for article in docs['hits']['hits']:
                spn_list.append({"spn":article['_source']["artnumber"].split('-')[1], "variantid":str(article['_source']['variantid'])})
        return spn_list
    #if the connection exception raise, wait for 15 seconds and retry
    except requests.exceptions.ConnectionError:
        time.sleep(15)
        return get_spn_list()

#amazon api call
def search_amazon(keyword, interval):
    try:
        az = amazon.api.AmazonAPI(AMAZON_ACCESS_KEY, AMAZON_SECRET_KEY, AMAZON_ASSOC_TAG, region='DE')
        product = az.lookup(ResponseGroup='OfferFull', ItemId=keyword)

        #return all of xml data
        return product
    #if the connection exception raise, wait for a second and retry
    except urllib2.HTTPError as e:
        time.sleep(interval)
        return search_amazon(keyword, interval)
    except urllib2.URLError as u:
        time.sleep(interval)
        return search_amazon(keyword, interval)

#send file to shop server at specific directory
def send_file(filename):
    paramiko.util.log_to_file('/tmp/paramiko.log')

    host = "94.130.50.93"
    port = 55555
    transport = paramiko.Transport((host, port))

    password = '!Geld7914'
    username = 'root'
    transport.connect(username = username, password = password)

    #to use sftp, import paramiko and make sftp client
    sftp = paramiko.SFTPClient.from_transport(transport)

    localpath = filename
    filepath = '/var/www/vhosts/my-mik.de/Sourcing_File/' + filename.split('/')[-1]
    sftp.put(localpath, filepath)

    sftp.close()
    transport.close()

#send a string to shop server to alarm the result file is sended
def send_signal():
    HOST = '94.130.50.93'
    PORT = 17914
    BUFSIZE = 1024
    ADDR = (HOST, PORT)

    client_socket = socket(AF_INET, SOCK_STREAM)

    try:
        client_socket.connect(ADDR)
        client_socket.send('AMZ_DE')

        #send terminate message after 5 seconds
        time.sleep(5)
        client_socket.send('connection terminate')
    except Exception as e:
        print e

#check article's option to acurate update or not
def check_update(product, variant):
    data = {
        'price':product.price_and_currency[0],
        'shipping':product._safe_get_element('Offers.Offer.OfferListing.IsEligibleForSuperSaverShipping'),
        'prime':product._safe_get_element('Offers.Offer.OfferListing.IsEligibleForPrime'),
        'merchant':product._safe_get_element('Offers.Offer.Merchant.Name')
    }

    #if price is None or 0, return
    if str(data['price']) == 'None' or data['price'] == 0:
        return

    #if shipping and prime are both None or 0, return
    elif (data['shipping'] == 0 and data['prime'] == 0) or (str(data['shipping'])=='None' and str(data['prime'])=='None'):
        return

    #else, update
    with open(RESULT_FILE, 'a') as f:
        f.write(variant + ',' + str(data['price']) + ',' + str(data['merchant']).replace("\"", "") + '\n')

#main update function
def price_process(spn_list, call_interval, interval_on_error):
    asin_list = []
    variant_list = {}

    for spn in spn_list:
        asin_list.append(spn['spn'])
        variant_list[spn['spn']] = spn['variantid']

        #If the length of list is less than 10, continue
        if len(asin_list) != 10:
            continue

        #When the length of list get 10, call amazon api by this list
        try:
            time.sleep(call_interval)
            article_list = search_amazon(','.join(asin_list), interval_on_error)
        #If all the asin number in list are not founded, reset list and continue
        except amazon.api.AsinNotFound as e:
            asin_list = []
            variant_list = {}
            continue
        #Response of amazon api call is saved as list
        #Check whether to update one by one
        if type(article_list) != type(asin_list):
            check_update(article_list, variant_list[article_list._safe_get_element('ASIN')])
        else:
            for product in article_list:
                try:
                    check_update(product, variant_list[product._safe_get_element('ASIN')])
                except:
                    continue
        asin_list = []
        variant_list = {}

def main():
    call_interval = 1
    interval_on_error = 12

    #reset result file and get amazon article's article list
    open(RESULT_FILE, 'w')
    spn_list = get_spn_list()

    price_process(spn_list, call_interval, interval_on_error)
    send_file(RESULT_FILE)
    send_signal()

    del spn_list

if __name__ == '__main__':
    if fork() > 0:
        exit(1)
    else:
        main()
