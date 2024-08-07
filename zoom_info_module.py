import pandas as pd
import requests
import concurrent.futures
from tqdm import tqdm
import cloudscraper
import glob
import time
import random
from constants import cookie_reference, browser_cookie, user_id

scraper = cloudscraper.create_scraper()

class zoominfo:
    def __init__(self):
        self.browser_cookie = browser_cookie.split(';')
        self.parsed_cookie = [element for element in self.browser_cookie if any(kw in element for kw in cookie_reference)]
        self.parsed_cookie = list(set(self.parsed_cookie))
        self.parsed_cookie = '; '.join(self.parsed_cookie).strip()

        self.ziid = self.parsed_cookie.split('ziid=')[1].split(';')[0]
        self.ziid = f'\"{self.ziid}\"'
        self.zisession = self.parsed_cookie.split('zisession=')[1].split(';')[0]
        self.zisession = f'\"{self.zisession}\"'

        self.headers = {
            'cookie': self.parsed_cookie,
            'user': user_id,
            'x-ziid': self.ziid,
            'x-zisession': self.zisession
        }
        
    def random_delay(self, start_int, end_int):
        delay = random.uniform(start_int, end_int)
        # print(f"Sleeping for {delay:.2f} seconds")
        time.sleep(delay)

    def get_contact_details(self, id):
        url = 'https://app.zoominfo.com/anura/userData/viewContacts'
        data = {"contacts":[{"personId":f"{id}"}],"creditSource":"GROW"}
        response = scraper.post(url, headers=self.headers, json=data)
        response_list = response.json()['data']#['personSearch']['basic']
        if len(response_list) != 0:
            response_list = response_list[0]
            response_dict = {key: [str(values)] for key, values in response_list.items()}
            temp_df = pd.DataFrame.from_dict(response_dict)
            email_name = temp_df['email'][0]
            temp_df.to_csv(f'temp/{email_name}.csv', index=False)
            return temp_df


    def search(self, firstName, lastName, company):
        url = 'https://app.zoominfo.com/profiles/graphql/personSearch'
        data = {
            "operationName": "personSearch",
            "variables": {
                "searchFacadeParams": {
                    "firstName": f"{firstName}",
                    "lastName": f"{lastName}",
                    "companyName":f"{company}",
                    "page": 1,
                    "rpp": 1,
                    "excludeBoardMembers": False
                },
                "includeIsEmailUnsubscribed": False
            },
            "query": "query personSearch($searchFacadeParams: PersonArgs, $includeIsEmailUnsubscribed: Boolean!) {\n  personSearch(searchFacadeParams: $searchFacadeParams) {\n    primary: data {\n      entityId: personID\n      doziIndustry {\n        displayName\n        name\n        isPrimary\n        score\n      }\n      topLevelIndustry\n      title\n      jobFunction\n      orgChartJobFunction {\n        department\n        departmentId\n        jobFunction\n        jobFunctionId\n      }\n      employmentHistory {\n        companyName\n        from\n        to\n        jobFunction\n        title\n        level\n        companyID\n        companyWebsite\n      }\n      education {\n        school\n        degree {\n          areaOfStudy\n          degree\n        }\n      }\n      webReference {\n        description\n        title\n        url\n        date\n      }\n      boardMember {\n        from\n        to\n        jobFunction\n        title\n        level\n        company {\n          id\n          name\n          tagged\n          masked\n          subscribed\n          exported\n          description\n          domain\n          logo\n          fax\n          phone\n          ticker\n          crmEntityId\n          address {\n            Street\n            City\n            State\n            Zip\n            street\n            city\n            state\n            zip\n            country\n            latitude\n            longitude\n            CountryCode\n          }\n          displayAddress\n          employeeCount\n          revenue\n          doziIndustry {\n            displayName\n            name\n            isPrimary\n            score\n          }\n          doziIndustryString\n          revenueRange\n          isDefunct\n          uniqueCompanyNumContacts\n          companyHref\n          isInPreview\n          funding {\n            amountIn000s\n            date\n            type\n            investors {\n              companyName\n            }\n          }\n          departmentBudgets {\n            departmentType\n            budgetAmount\n          }\n          certified\n          certificationDate\n          icpScore\n          locationsCount\n          ultimateParent: basicUltimateParent\n          orgImport\n        }\n      }\n      personBiography\n      socialUrls {\n        socialMedia {\n          socialNetworkType\n          socialNetworkUrl\n        }\n      }\n      socialUrlsParsed {\n        linkedin\n        facebook\n        twitter\n        youtube\n      }\n      followerCountParsed {\n        linkedin\n        facebook\n        twitter\n        youtube\n      }\n      foundedYear\n      alexaRank\n      directPhoneIsDoNotCall\n      mobilePhoneIsDoNotCall\n      emailBlockedReason\n      directPhoneBlockedReason\n      mobilePhoneBlockedReason\n      personalEmailBlockedReason\n      company {\n        id\n      }\n      importedData {\n        date\n        owners {\n          key\n          value {\n            date\n            crmEntityId\n            ownerId\n            ownerName\n          }\n        }\n      }\n      personHashtags\n      isEmailUnsubscribed @include(if: $includeIsEmailUnsubscribed)\n    }\n    basic: data {\n      name\n      id: personID\n      image: profileImageURL\n      firstName\n      lastName\n      email\n      phone\n      personalEmail\n      timezone\n      mobilePhone\n      businessEmailBlocked: emailBlocked\n      personalEmailBlocked\n      mobilePhoneBlocked\n      directPhoneBlocked\n      emailBlockedReason\n      directPhoneBlockedReason\n      mobilePhoneBlockedReason\n      personalEmailBlockedReason\n      masked: isMasked\n      tagged: isTagged\n      isTracked\n      title\n      lastUpdateDate: lastUpdatedDate\n      lastMentioned\n      confidence: confidenceScore\n      orgUniversalTagged {\n        tagName\n        value\n      }\n      universalTagged {\n        tagName\n        value\n      }\n      noticeProvidedInfo {\n        emailNoticeProvidedDate\n      }\n      buyingCommittee\n      socialUrls {\n        socialMedia {\n          socialNetworkType\n          socialNetworkUrl\n        }\n      }\n      socialUrlsParsed {\n        linkedin\n        facebook\n        twitter\n        youtube\n      }\n      isUnEmployed: isPast\n      companyID\n      companyLogo\n      companyName\n      companyAddress {\n        Street\n        City\n        State\n        Zip\n        CountryCode\n        street\n        city\n        state\n        zip\n        country\n        latitude\n        longitude\n      }\n      companyRevenue\n      companyRevenueRange\n      companyEmployees\n      companyEmployeeCountRange\n      companyDomain\n      companyPhone\n      companyDescription\n      companyFax\n      companyRevenueIn000s\n      companySIC\n      companyNAICS\n      companyTicker\n      topLevelIndustry\n      industry\n      doziIndustry {\n        displayName\n        name\n        isPrimary\n        score\n      }\n      creationDate\n      positionStartDate\n      hasOnlinePresence\n      publicSourcedData {\n        dataType\n        urls\n      }\n      directPhoneIsDoNotCall\n      mobilePhoneIsDoNotCall\n      personHashtags\n      address: location {\n        city: City\n        country: CountryCode\n        state: State\n        street: Street\n        zip: Zip\n        metroArea: metroArea\n      }\n      isEmailUnsubscribed @include(if: $includeIsEmailUnsubscribed)\n    }\n  }\n}\n"
        }

        response = scraper.post(url, headers=self.headers, json=data)

        if response.status_code != 200:
            # Append the error message to the log file
            error_message = f"Request Response Code {firstName} : {company}: {response.status_code}\n"

        response_list = response.json()['data']['personSearch']['basic']
        if len(response_list) != 0:
            response_list = response_list[0]
            # response_dict = {key: [str(values)] for key, values in response_list.items()}
            # return pd.DataFrame.from_dict(response_dict)
            contact_id = self.get_contact_details(response_list['id'])
            self.random_delay(45,75)
            return contact_id
        else:
            self.random_delay(6,13)

    def search_email(self, emailAddress):
        url = 'https://app.zoominfo.com/profiles/graphql/personSearch'
        data = {
            "operationName": "personSearch",
            "variables": {
                "searchFacadeParams": {
                    "emailAddress": f"{emailAddress}",
                    "page": 1,
                    "rpp": 1,
                    "excludeBoardMembers": False
                },
                "includeIsEmailUnsubscribed": False
            },
            "query": "query personSearch($searchFacadeParams: PersonArgs, $includeIsEmailUnsubscribed: Boolean!) {\n  personSearch(searchFacadeParams: $searchFacadeParams) {\n    primary: data {\n      entityId: personID\n      doziIndustry {\n        displayName\n        name\n        isPrimary\n        score\n      }\n      topLevelIndustry\n      title\n      jobFunction\n      orgChartJobFunction {\n        department\n        departmentId\n        jobFunction\n        jobFunctionId\n      }\n      employmentHistory {\n        companyName\n        from\n        to\n        jobFunction\n        title\n        level\n        companyID\n        companyWebsite\n      }\n      education {\n        school\n        degree {\n          areaOfStudy\n          degree\n        }\n      }\n      webReference {\n        description\n        title\n        url\n        date\n      }\n      boardMember {\n        from\n        to\n        jobFunction\n        title\n        level\n        company {\n          id\n          name\n          tagged\n          masked\n          subscribed\n          exported\n          description\n          domain\n          logo\n          fax\n          phone\n          ticker\n          crmEntityId\n          address {\n            Street\n            City\n            State\n            Zip\n            street\n            city\n            state\n            zip\n            country\n            latitude\n            longitude\n            CountryCode\n          }\n          displayAddress\n          employeeCount\n          revenue\n          doziIndustry {\n            displayName\n            name\n            isPrimary\n            score\n          }\n          doziIndustryString\n          revenueRange\n          isDefunct\n          uniqueCompanyNumContacts\n          companyHref\n          isInPreview\n          funding {\n            amountIn000s\n            date\n            type\n            investors {\n              companyName\n            }\n          }\n          departmentBudgets {\n            departmentType\n            budgetAmount\n          }\n          certified\n          certificationDate\n          icpScore\n          locationsCount\n          ultimateParent: basicUltimateParent\n          orgImport\n        }\n      }\n      personBiography\n      socialUrls {\n        socialMedia {\n          socialNetworkType\n          socialNetworkUrl\n        }\n      }\n      socialUrlsParsed {\n        linkedin\n        facebook\n        twitter\n        youtube\n      }\n      followerCountParsed {\n        linkedin\n        facebook\n        twitter\n        youtube\n      }\n      foundedYear\n      alexaRank\n      directPhoneIsDoNotCall\n      mobilePhoneIsDoNotCall\n      emailBlockedReason\n      directPhoneBlockedReason\n      mobilePhoneBlockedReason\n      personalEmailBlockedReason\n      company {\n        id\n      }\n      importedData {\n        date\n        owners {\n          key\n          value {\n            date\n            crmEntityId\n            ownerId\n            ownerName\n          }\n        }\n      }\n      personHashtags\n      isEmailUnsubscribed @include(if: $includeIsEmailUnsubscribed)\n    }\n    basic: data {\n      name\n      id: personID\n      image: profileImageURL\n      firstName\n      lastName\n      email\n      phone\n      personalEmail\n      timezone\n      mobilePhone\n      businessEmailBlocked: emailBlocked\n      personalEmailBlocked\n      mobilePhoneBlocked\n      directPhoneBlocked\n      emailBlockedReason\n      directPhoneBlockedReason\n      mobilePhoneBlockedReason\n      personalEmailBlockedReason\n      masked: isMasked\n      tagged: isTagged\n      isTracked\n      title\n      lastUpdateDate: lastUpdatedDate\n      lastMentioned\n      confidence: confidenceScore\n      orgUniversalTagged {\n        tagName\n        value\n      }\n      universalTagged {\n        tagName\n        value\n      }\n      noticeProvidedInfo {\n        emailNoticeProvidedDate\n      }\n      buyingCommittee\n      socialUrls {\n        socialMedia {\n          socialNetworkType\n          socialNetworkUrl\n        }\n      }\n      socialUrlsParsed {\n        linkedin\n        facebook\n        twitter\n        youtube\n      }\n      isUnEmployed: isPast\n      companyID\n      companyLogo\n      companyName\n      companyAddress {\n        Street\n        City\n        State\n        Zip\n        CountryCode\n        street\n        city\n        state\n        zip\n        country\n        latitude\n        longitude\n      }\n      companyRevenue\n      companyRevenueRange\n      companyEmployees\n      companyEmployeeCountRange\n      companyDomain\n      companyPhone\n      companyDescription\n      companyFax\n      companyRevenueIn000s\n      companySIC\n      companyNAICS\n      companyTicker\n      topLevelIndustry\n      industry\n      doziIndustry {\n        displayName\n        name\n        isPrimary\n        score\n      }\n      creationDate\n      positionStartDate\n      hasOnlinePresence\n      publicSourcedData {\n        dataType\n        urls\n      }\n      directPhoneIsDoNotCall\n      mobilePhoneIsDoNotCall\n      personHashtags\n      address: location {\n        city: City\n        country: CountryCode\n        state: State\n        street: Street\n        zip: Zip\n        metroArea: metroArea\n      }\n      isEmailUnsubscribed @include(if: $includeIsEmailUnsubscribed)\n    }\n  }\n}\n"
        }

        response = scraper.post(url, headers=self.headers, json=data)

        if response.status_code != 200:
            # Append the error message to the log file
            error_message = f"Request Response Code {emailAddress}: {response.status_code}\n"

        response_list = response.json()['data']['personSearch']['basic']
        if len(response_list) != 0:
            response_list = response_list[0]
            # response_dict = {key: [str(values)] for key, values in response_list.items()}
            # return pd.DataFrame.from_dict(response_dict)
            contact_id = self.get_contact_details(response_list['id'])
            self.random_delay(45,75)
            return contact_id
        else:
            self.random_delay(6,13)            