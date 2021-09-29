import requests
from ordered_set import OrderedSet
from bs4 import BeautifulSoup
from multiprocessing import Pool
from multiprocessing import cpu_count
import time
import sys
from lxml import etree
import pandas as pd
import json
result=[]
def solve(url):
  html=requests.get(url,headers = {'accept': 'application/json','Accept-Language' : 'hi_IN','User-Agent': 'Mozilla/4.0'}).text
  dom = etree.HTML(html)
  if len(dom.xpath("//h1/text()"))==0:
    return
  dict1={}
  dict1['Title']=''.join(dom.xpath("//h1/text()"))
  dict1['Pages']=''.join(dom.xpath("//p[@class='note note-list']/span[1]/text()"))
  dict1['Posted']=''.join(dom.xpath("//p[@class='note note-list']/span[2]/text()"))
  dict1['Author']=''.join(dom.xpath("//a[@title='View other papers by this author']/text()"))
  dict1['Date Written']=''.join(dom.xpath("//div[@class='authors authors-full-width']/../p/text()"))
  dict1['University']=''.join(dom.xpath("//a[@title='View other papers by this author']/../p/text()"))
  dict1['Downloads']=''.join(dom.xpath("(//div[@class='stat col-lg-4']/div[@class='number'])[1]/text()"))
  dict1['Abstract Views']=''.join(dom.xpath("(//div[@class='stat col-lg-4']/div[@class='number'])[2]/text()"))
  dict1['Rank']=''.join(dom.xpath("(//div[@class='stat col-lg-4']/div[@class='number'])[3]/text()"))
  dict1['Citation']=''.join(dom.xpath("//span[@class='number-total-citations']/text()"))
  dict1['Refrences']=''.join(dom.xpath("//a[@href='#references-widget']/span[1]/text()"))
  dict1['Abstract']=''.join(dom.xpath("//div[@class='abstract-text']/p/text()"))
  dict1['Keywords']=''.join(dom.xpath("//div[@class='box-container box-abstract-main']/p/strong[contains(text(),'Key')]/../text()"))
  dict1['Citation']=''.join(dom.xpath("//div[@class='suggested-citation']/text()"))
  dict1['Citation Links']=''.join(dom.xpath("//div[@class='suggested-citation']/a/text()"))
  dict1['Download Link']=''.join(dom.xpath("(//a[@class='button-link primary '])[1]/@href"))
  dict1['Online PDF Link']=''.join(dom.xpath("(//a[@class='button-link secondary '])[1]/@href"))
  dict1['Url']=url
  dict1['Related Journals']=','.join(dom.xpath("//li[@class='related-journal']/p/a/text()"))
  dict1['Related Journals Link']=','.join(dom.xpath("//li[@class='related-journal']/p/a/@href"))
  dict1['JEL Classification']=''.join(dom.xpath("//p/strong[contains(text(),'JEL')]/../text()"))
  sss=requests.get('https://api.plu.mx/widget/other/artifact?type=ssrn_id&id='+url.split('=')[1]).text
  x=json.loads(sss)
  count=0
  ex=0
  try:
   for cn in x['statistics']['Captures']:
    if cn['label']=='Readers':
     count+=cn['count']
    if cn['label']=='Exports-Saves':
     ex+=cn['count']
  except:
    pass
  ns=0
  try:
   for news_mention in x['statistics']['Mentions']:
    ns+=news_mention['count']
  except:
    pass
  fb=0
  twitter=0
  try:
   for social_media in x['statistics']['Social Media']:
    if social_media['label']=='Shares, Likes & Comments':
     fb+=social_media['count']
    if social_media['label']=='Tweets':
     twitter+=social_media['count']
  except:
     pass

  dict1['Readers']=count
  dict1['Exports Save']=ex
  dict1['News Mentions']=ns
  dict1['Share, Likes and comments']=fb
  dict1['Tweets']=twitter


  #print(dict1)
  #result.append(dict1)
  return dict1
def quickSoup(url):
    try:
        header = {}
        header['User-Agent'] = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'
        # page = requests.get(url, headers=header, timeout=10)
        soup = BeautifulSoup(requests.get(url, headers=header, timeout=10).content, 'html.parser')
        return(soup)
    except Exception:
        return(None)



def dummyscrape(start, stop):
    numWorkers = cpu_count() * 12
    print(numWorkers)
    p = Pool(numWorkers)
    linkList = ["https://papers.ssrn.com/sol3/papers.cfm?abstract_id=" + str(x) for x in range(start, stop)]

    papers = p.map(getPaper, linkList)
    p.terminate()
    p.join()


def scrape(start, stop):
    numWorkers = cpu_count() * 12
    p = Pool(numWorkers)
    linkList = ["https://papers.ssrn.com/sol3/papers.cfm?abstract_id={}".format(str(x)) for x in range(start, stop)]

    papers = p.map(solve, linkList)
    p.terminate()
    p.join()
    return papers

breaks = [10000 * x for x in range(389, 391)]

t = time.time()
paper=[]
#for i in range(1):
for i in range(len(breaks)-1):
        print(breaks[i]) 
        b = time.time()
        #paper.append(scrape(breaks[i], breaks[i+1]))
        paper.append(scrape(3440000,3440049))
        print("TIME FOR 1000: " + str(time.time() - b))
        print("TIME SINCE START: " + str(time.time() - t))

result=[]
for i in paper:
  for j in i:
    if str(j)!='None':
       if j['Abstract Views'] != '':
            print(j)
            result.append(j)
#print(result)
dataset=pd.DataFrame(result)
dataset.to_csv("Output.csv")