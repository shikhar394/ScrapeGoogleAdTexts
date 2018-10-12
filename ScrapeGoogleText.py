import json 
import requests
import configparser
import psycopg2
import psycopg2.extras
import sys
from email.mime.text import MIMEText
import smtplib

if len(sys.argv) < 2:
    exit("Usage:python3 ScrapeGoogleText.py ScrapeGoogleText.cfg")

config = configparser.ConfigParser()
config.read(sys.argv[1])

ERROREMAIL = config['ACCOUNT']['SENDERROREMAIL']
SENDERROREMAIL = config['ACCOUNT']['SENDERROREMAIL']
SENDERRORPASS = config['ACCOUNT']['SENDERRORPASS']
JSON_INDENT = int(config['SPECS']['JSON_INDENT'])

HOST = config['POSTGRES']['HOST']
DBNAME = config['POSTGRES']['DBNAME']
USER = config['POSTGRES']['USER']
PASSWORD = config['POSTGRES']['PASSWORD']
DBAuthorize = "host=%s dbname=%s user=%s password=%s" % (HOST, DBNAME, USER, PASSWORD)
connection = psycopg2.connect(DBAuthorize)
cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)

CREATIVE_ID_POS = -1
ADVERTISER_ID_POS = -3 

SAMPLELINKS = ["https://transparencyreport.google.com/political-ads/library/advertiser/AR10924747533582336/creative/CR405266791858700288", 
    "https://transparencyreport.google.com/political-ads/library/advertiser/AR194446432348930048/creative/CR168700268072927232", 
    "https://transparencyreport.google.com/political-ads/library/advertiser/AR201201831789985792/creative/CR107466266498826240", 
    "https://transparencyreport.google.com/political-ads/library/advertiser/AR201201831789985792/creative/CR177940563792756736", 
    "https://transparencyreport.google.com/political-ads/library/advertiser/AR201201831789985792/creative/CR187840566489251840"]

LINKWITHTEXT = 'https://transparencyreport.google.com/transparencyreport/api/v3/politicalads/creatives/details?entity_id=%s&creative_id=%s'





def ExtractIDs(ad_url):
    """
    URLs in the form of 
    https://transparencyreport.google.com/political-ads/library/advertiser/AR201201831789985792/creative/CR187840566489251840

    Extracts the AR/CR tokens from the link. 
    """
    ad_url = ad_url.split('/')
    if ad_url[-3].startswith('AR') and ad_url[-1].startswith('CR'):
        return ad_url[ADVERTISER_ID_POS], ad_url[CREATIVE_ID_POS]
    else:
        for token in ad_url:
            if token.startswith('AR'):
                AdvertiserID = token
            elif token.startswith('CR'):
                CreativeID = token
        return AdvertiserID, CreativeID





def GetLinks():    
    """
    Gets all the URLs from the DB to extract their text. 
    """
    Query = "select ad_url from creative_stats"
    cursor.execute(Query)
    Links = []
    for ad_url in cursor:
        AdvertiserID, CreativeID = ExtractIDs(ad_url)
        Links.append(LINKWITHTEXT % (AdvertiserID, CreativeID))
    return Links





def CacheExistingAdIDs(AdvertisementDetails):
    """
    Caches all the ad_ids that exists in the DB so we don't crawl them again. 
    """
    Query = "select advertisement_id from ad_copies"
    cursor.execute(Query)
    for row in cursor:
        AdvertisementDetails[row['advertisement_id']] = -1
    




def SendErrorEmail(ErrorMessage):
    msg = MIMEText(str(ErrorMessage))
    msg['from'] = SENDERROREMAIL
    msg['to'] = ERROREMAIL
    msg['subject'] = 'Error in getting tweets script'
    s = smtplib.SMTP('smtp.live.com', 25)
    s.ehlo()
    s.starttls()
    s.login(SENDERROREMAIL, SENDERRORPASS)
    s.sendmail(SENDERROREMAIL, [ERROREMAIL], msg.as_string())
    s.quit()





def FlattenDataHelper(Payload, Data):
    for element in Payload:
        if type(element) == list:
            FlattenDataHelper(element, Data)
        else:
            Data.append(element)





def FlattenData(Payload):
    """
    As of 10/13/18, the data parsed by Google for their ad pages exists as a 
    convoluted multi-layered lists. This function flattens it so we can access
    the elements easily.
    The data also starts with ')]}' 
    """
    Data = []
    if Payload.startswith(')]}'):
        Payload = Payload[len(")]}'"):]
    Payload = json.loads(Payload)
    FlattenDataHelper(Payload, Data)
    return Data





def ExtractRelevantText(Payload):
    """
    Picks out the text relevant for the db entry from all the data returned from the ad page.
    """
    RelevantPayload = []
    for element in Payload:
        if element != "pa.cdr" and isinstance(element, str):
            RelevantPayload.append(element)
    return RelevantPayload





def CategorizeText(RelevantPayload):
    """
    Categorizes the text returned from the website. 
    As of 10/13/18, the last element is the link, the one before that is the body, the rest is the title. 
    """
    AdvertiserLink = RelevantPayload.pop()
    Body = RelevantPayload.pop()
    Title = ' | '.join(RelevantPayload)
    return Title, Body, AdvertiserLink





def InsertNewEntriesToDB(AdvertisementCopies):
    """
    Batch inserts all the data to the db.
    """
    Query = "INSERT into ad_copies (advertisement_id, advertiser_id, title, body, advertiser_link) VALUES "
    Params = []
    for AdvertisementID in AdvertisementCopies:
        if AdvertisementCopies[AdvertisementID] != -1:
            Title = AdvertisementCopies[AdvertisementID]['Title']
            Body = AdvertisementCopies[AdvertisementID]['Body']
            AdvertiserLink = AdvertisementCopies[AdvertisementID]['AdvertiserLink']
            Params.append(cursor.mogrify("(%s, %s, %s, %s, %s)", (AdvertisementID, Title, Body, AdvertiserLink)))
    Query += ','.join(Params)
    print(Query)
    cursor.execute(Query)
    connection.commit()





if __name__ == "__main__":
    LinksFromDB = GetLinks()
    if LinksFromDB:
        AdvertisementCopies = {} # {AdvertisementID: {'Title': 'XXXX', 'Body': 'XXXX', 'AdvertiserLink': 'XXXX', 'AdvertiserID': 'XXX'}}
        CacheExistingAdIDs(AdvertisementCopies)
        with requests.session() as Session:
            for Link in SAMPLELINKS:
                AdvertiserID, AdvertisementID = ExtractIDs(Link)
                if not AdvertisementCopies.get(AdvertisementID, False):
                    LinkToScrape = LINKWITHTEXT % (AdvertiserID, AdvertisementID) 
                    try:
                        Payload = Session.get(LinkToScrape)
                        if Payload.status_code != 200:
                            SendErrorEmail("Not 200 code on " + Link)
                    except Exception as e:  
                        SendErrorEmail("Error: " + str(e))
                    Payload = FlattenData(Payload.text)
                    RelevantPayload = ExtractRelevantText(Payload)
                    Title, Body, AdvertiserLink = CategorizeText(RelevantPayload)
                    AdvertisementCopies[AdvertisementID] = {
                        'Title': Title,
                        'Body': Body,
                        'AdvertiserLink': AdvertiserLink,
                        'AdvertiserID': AdvertiserID
                    }
        
        InsertNewEntriesToDB(AdvertisementCopies)
        connection.close()

