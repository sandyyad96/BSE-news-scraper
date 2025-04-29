import os

import sys

import time

import pandas as pd

import logging

from datetime import datetime

import requests

from bs4 import BeautifulSoup

from lxml import etree

import re

from urllib.parse import urlparse, urljoin



# Setup logging

logging.basicConfig(

    level=logging.INFO,

    format='%(asctime)s - %(levelname)s - %(message)s',

    handlers=[

        logging.FileHandler("bse_fetcher.log"),

        logging.StreamHandler()

    ]

)



# Setup paths

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from bse import BSE



download_folder = os.path.join(os.getcwd(), 'downloads')

os.makedirs(download_folder, exist_ok=True)



# Initialize BSE

bse = BSE(download_folder=download_folder)



# User-Agent for requests

HEADERS = {

    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',

    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',

    'Accept-Language': 'en-US,en;q=0.5',

    'Connection': 'keep-alive',

    'Upgrade-Insecure-Requests': '1',

}



# Base URLs

BSE_BASE = "https://www.bseindia.com"



def fix_pdf_url(url):

    """Fix the duplicated base URL issue."""

    if not url:

        return ""

        

    # Check for doubled domain

    if url.count("bseindia.com") > 1:

        # Extract the path part (everything after the second occurrence of bseindia.com)

        match = re.search(r"bseindia\.com(.*?)bseindia\.com(.+)", url)

        if match:

            return f"https://www.bseindia.com{match.group(2)}"

    

    # Ensure URL starts with proper scheme

    if url and not url.startswith(('http://', 'https://')):

        return f"https://www.bseindia.com{url if url.startswith('/') else '/' + url}"

        

    return url



def extract_xbrl_id_from_page(news_id):

    """Extract the XBRL ID from the announcement page."""

    try:

        url = f"{BSE_BASE}/corporates/anndet_new.aspx?newsid={news_id}"

        res = requests.get(url, headers=HEADERS, timeout=10)

        

        if res.status_code != 200:

            logging.warning(f"Failed to fetch announcement page for {news_id}: Status {res.status_code}")

            return None

            

        # Look for pattern in page source that contains XBRL file reference

        match = re.search(r"AttachLive/([a-zA-Z0-9\-]+)\.xml", res.text)

        if match:

            return match.group(1)

            

        return None

    except Exception as e:

        logging.error(f"Error extracting XBRL ID for NEWSID {news_id}: {e}")

        return None



def extract_from_xbrl(xbrl_id):

    """Extract PDF link and category from XBRL file."""

    if not xbrl_id:

        return None, None, None

        

    try:

        url = f"{BSE_BASE}/xml-data/corpfiling/AttachLive/{xbrl_id}.xml"

        res = requests.get(url, headers=HEADERS, timeout=10)

        

        if res.status_code != 200:

            logging.warning(f"Failed to fetch XBRL file {xbrl_id}: Status {res.status_code}")

            return None, None, None

            

        root = etree.fromstring(res.content)

        

        # Extract PDF link from AttachmentURL

        pdf_links = root.xpath("//in-bse-co:AttachmentURL/text()")

        attachment_url = pdf_links[0] if pdf_links else ""

        pdf_link = fix_pdf_url(attachment_url)

        

        # Enhanced patterns for category extraction

        category = None

        category_patterns = [

            "//in-bse-co:CategoryOfAnnouncement/text()",

            "//in-bse-co:TypeOfAnnouncement/text()",

            "//CategoryOfAnnouncement/text()",

            "//TypeOfAnnouncement/text()",

            "//xbrl:CategoryOfAnnouncement/text()",

            "//bse-coi:CategoryName/text()",  # For newer XBRL format

            "//AnnouncementType/text()"

        ]

        

        for pattern in category_patterns:

            elements = root.xpath(pattern)

            if elements and elements[0].strip():

                category = elements[0].strip()

                break

                

        # Enhanced patterns for subcategory extraction

        subcategory = None

        subcategory_patterns = [

            "//in-bse-co:SubjectOfAnnouncement/text()",

            "//in-bse-co:SubCategoryOfAnnouncement/text()",

            "//SubjectOfAnnouncement/text()",

            "//SubCategoryOfAnnouncement/text()",

            "//xbrl:SubCategoryOfAnnouncement/text()",

            "//bse-coi:SubCategoryName/text()",  # For newer format

            "//in-bse-co:AcquisitionDetails/text()",  # For acquisition specifically

            "//AcquisitionOrDisposalAnnouncement/text()"

        ]

        

        for pattern in subcategory_patterns:

            elements = root.xpath(pattern)

            if elements and elements[0].strip():

                subject_text = elements[0].strip()

                # Parse the subject for subcategory

                if "-" in subject_text:

                    parts = subject_text.split("-", 1)

                    if "Regulation" in parts[0] or "LODR" in parts[0]:

                        subcategory = parts[1].strip()

                    else:

                        subcategory = subject_text

                else:

                    subcategory = subject_text

                break

                

        return pdf_link, category, subcategory

    except Exception as e:

        logging.error(f"XBRL error for ID {xbrl_id}: {e}")

        return None, None, None



def extract_from_dropdown(soup):

    """Extract the selected subcategory from dropdown if present."""

    # Try to find select element

    subcategory_select = soup.find("select", id=lambda x: x and "subcategory" in x.lower())

    if subcategory_select:

        # Find selected option

        selected_option = subcategory_select.find("option", selected=True)

        if selected_option and selected_option.text.strip():

            return selected_option.text.strip()

    

    # Try different dropdown identification methods

    all_selects = soup.find_all("select")

    for select in all_selects:

        if "category" in str(select).lower():

            selected_option = select.find("option", selected=True)

            if selected_option and selected_option.text.strip():

                return selected_option.text.strip()

    

    return None



def extract_from_hidden_fields(soup):

    """Extract category info from hidden fields in the page."""

    # Look for all hidden input fields

    hidden_fields = soup.find_all("input", type="hidden")

    

    category = None

    subcategory = None

    

    for field in hidden_fields:

        field_name = field.get("name", "").lower()

        field_value = field.get("value", "").strip()

        

        if not field_value:

            continue

            

        if "category" in field_name and "sub" not in field_name:

            category = field_value

        elif "subcategory" in field_name or ("category" in field_name and "sub" in field_name):

            subcategory = field_value

    

    return category, subcategory





def extract_from_page(news_id):

    """Extract information directly from the announcement page."""

    try:

        url = f"{BSE_BASE}/corporates/anndet_new.aspx?newsid={news_id}"

        res = requests.get(url, headers=HEADERS, timeout=10)

        

        if res.status_code != 200:

            logging.warning(f"Failed to fetch announcement page for {news_id}: Status {res.status_code}")

            return None, None, None

            

        soup = BeautifulSoup(res.content, 'html.parser')

        

        # Extract PDF

        pdf_link = ""

        for link in soup.find_all('a', href=True):

            if ".pdf" in link['href'].lower():

                pdf_link = fix_pdf_url(link['href'])

                break

        

        # Try getting from hidden fields first

        hidden_category, hidden_subcategory = extract_from_hidden_fields(soup)

        

        # Get main category - Check various IDs used on the BSE website

        category = hidden_category

        if not category:

            # Additional category selectors

            category_selectors = [

                "span[id*='lblCat']",

                "span[id*='Category']",

                ".announcement-category", 

                "#ctl00_ContentPlaceHolder1_lblCategory"

            ]

            

            for selector in category_selectors:

                cat_elems = soup.select(selector)

                for cat_elem in cat_elems:

                    if cat_elem and cat_elem.text.strip():

                        category = cat_elem.text.strip()

                        break

                if category:

                    break

        

        # Get subcategory - First try dropdown

        subcategory = hidden_subcategory

        if not subcategory:

            dropdown_subcategory = extract_from_dropdown(soup)

            if dropdown_subcategory:

                subcategory = dropdown_subcategory

        

        # If not found in dropdown, check various IDs

        if not subcategory:

            subcategory_selectors = [

                "span[id*='SubCat']",

                "span[id*='SubCategory']",

                ".announcement-subcategory",

                "#ctl00_ContentPlaceHolder1_lblSubCategory", 

                "select[id*='ddlSubCategory'] option[selected]"

            ]

            

            for selector in subcategory_selectors:

                subcat_elems = soup.select(selector)

                for subcat_elem in subcat_elems:

                    if subcat_elem and subcat_elem.text.strip():

                        subcategory = subcat_elem.text.strip()

                        break

                if subcategory:

                    break

                

        # If still no subcategory, try from announcement text

        if not subcategory:

            announcement_text = soup.get_text()

            subcat_patterns = [

                r"Sub[- ]?[Cc]ategory\s*:\s*([^\n]+)", 

                r"Subject\s*:\s*([^\n]+)",

                r"Type\s*:\s*([^\n]+)",

                r"Acquisition\s*:\s*([^\n]+)",

                r"Regulation\s+30.*?-\s*(.*?)(?:\s*$|\s*\.)",

                r"LODR.*?-\s*(.*?)(?:\s*$|\s*\.)"

            ]

            

            for pattern in subcat_patterns:

                match = re.search(pattern, announcement_text)

                if match:

                    subcategory = match.group(1).strip()

                    break

        

        return pdf_link, category, subcategory

    except Exception as e:

        logging.error(f"Error extracting from page for NEWSID {news_id}: {e}")

        return None, None, None



def get_pdf_and_categories(news_id):

    """Get PDF link and announcement categories using multiple methods."""

    # First try extracting directly from the page

    pdf_link, category, subcategory = extract_from_page(news_id)

    

    # If we're missing information, try getting the XBRL ID and extracting from XBRL

    if not pdf_link or not category or not subcategory or category == "General Announcement" or subcategory == "General":

        xbrl_id = extract_xbrl_id_from_page(news_id)

        if xbrl_id:

            pdf_link_xbrl, category_xbrl, subcategory_xbrl = extract_from_xbrl(xbrl_id)

            

            if not pdf_link and pdf_link_xbrl:

                pdf_link = pdf_link_xbrl

                

            if (not category or category == "Uncategorized" or category == "General Announcement") and category_xbrl:

                category = category_xbrl

                

            if (not subcategory or subcategory == "General") and subcategory_xbrl:

                subcategory = subcategory_xbrl

    

    # Apply category mapping for both category and subcategory

    if category:

        category = map_category(category)

    

    if subcategory:

        subcategory = map_subcategory(subcategory)

    

    # Default values if still empty

    pdf_link = pdf_link or ""

    category = category or "General Announcement"

    subcategory = subcategory or "General"

    

    return pdf_link, category, subcategory





def map_category(text):

    """Map category text to standard categories."""

    if not text:

        return "General Announcement"

        

    text = text.lower()

    

    # Common main categories

    if "regulation 30" in text or "lodr" in text:

        return "Announcement under Regulation 30 (LODR)"

    elif "general" in text:

        return "General Announcement"

    elif "board meeting" in text:

        return "Board Meeting"

    elif "financial result" in text:

        return "Financial Results"

    elif "agm" in text or "annual general" in text:

        return "AGM/EGM"

    elif "egm" in text or "extraordinary" in text:

        return "AGM/EGM"

    elif "dividend" in text:

        return "Dividend"

    elif "investor" in text or "presentation" in text:

        return "Investor Presentation"

    

    return text.title()



def map_subcategory(text):

    """Map text to standard subcategories based on BSE's dropdown menu."""

    if not text:

        return "General"

        

    text = text.lower()

    

    # Direct mapping from BSE dropdown menu - based on the screenshot

    subcategory_mapping = {

        "acquisition": "Acquisition",

        "agreement": "Agreement",

        "allotment of equity": "Allotment of Equity Shares",

        "allotment of warrant": "Allotment of Warrants",

        "award of order": "Award of Order / Receipt of Order",

        "receipt of order": "Award of Order / Receipt of Order",

        "buy back": "Buy back",

        "change in director": "Change in Directorate",

        "change in registered": "Change in Registered Office",

        "clarification": "Clarification",

        "declaration of nav": "Declaration of NAV",

        "delisting": "Delisting",

        "fccb": "FCCBs",

        "joint venture": "Joint Venture",

        "open offer": "Open Offer",

        "press release": "Press Release / Media Release",

        "media release": "Press Release / Media Release",

        "sale of share": "Sale of shares",

        "strike": "Strike",

        "utilisation of fund": "Utilisation of Funds",

        "debt securit": "Debt Securities",

        "credit rating": "Credit Rating",

        "change of name": "Change of Name",

        "shareholding": "Shareholding",

        "investor meet": "Analyst / Investor Meet",

        "analyst": "Analyst / Investor Meet",

        "investor complaint": "Reg. 13(3) - Statement of Investor Complaints",

        "compliance certificate": "Reg. 7(3) – Compliance Certificate",

        "pcs certificate": "Reg. 40 (10) - PCS Certificate",

        "deviation": "Reg. 32 (1), (3) - Statement of Deviation & Variation",

        "clarification of news": "Clarification of News Item",

        "disclosure under clause": "Disclosure under Clause 35A of the Listing Agreement",

        "nav declaration": "NAV Declaration",

        "appointment of director": "Appointment of Director",

        "appointment of chairman": "Appointment of Chairman",

        "appointment of managing director": "Appointment of Managing Director",

        "appointment of ceo": "Appointment of Chief Executive Officer (CEO)",

        "appointment of chief executive": "Appointment of Chief Executive Officer (CEO)",

        "appointment of cfo": "Appointment of Chief Financial Officer (CFO)",

        "appointment of chief financial": "Appointment of Chief Financial Officer (CFO)",

        "acquire": "Acquisition",

        "merger": "Acquisition",

        "purchase of": "Acquisition",

        "buying": "Acquisition",

        "acquired": "Acquisition",

        "lodr-acquisition": "Acquisition",

        "regulation 30-acquisition": "Acquisition"

    }

    

    # Check for direct keyword matches

    for key, value in subcategory_mapping.items():

        if key in text:

            return value

    

    # Handle very specific announcement types for acquisition

    if any(keyword in text for keyword in ["acquisition", "acquire", "merger", "take over", "buyout"]):

        return "Acquisition"

    

    # For regulation 30 announcements, try to extract from pattern

    if "regulation 30" in text or "lodr" in text:

        reg30_pattern = r"(?:Regulation\s+30|LODR)[^-]*-\s*(.*?)(?:\s*$|\s*\.)"

        match = re.search(reg30_pattern, text, re.IGNORECASE)

        if match:

            extracted = match.group(1).strip()

            # Try to map the extracted text

            return map_subcategory(extracted)

    

    # If text is very short and doesn't match anything, return it capitalized

    if len(text) < 30:

        words = text.split()

        if len(words) <= 4:

            return text.title()

    

    # Default to General if we can't determine a specific subcategory

    return "General"



    

    # First check exact matches in the subject line

    for key, value in subcategory_mapping.items():

        if key in text:

            return value

    

    # Handle specific keywords in context

    if "acqui" in text:

        return "Acquisition"

    elif "agre" in text:

        return "Agreement"

    elif "offer" in text and ("open" in text or "public" in text):

        return "Open Offer"

    elif "press" in text or "media" in text or "release" in text:

        return "Press Release / Media Release"

    elif "director" in text:

        return "Change in Directorate"

    

    # If text is very short and doesn't match anything, return it capitalized

    if len(text) < 30:

        return text.title()

    

    # Default to General if we can't determine a specific subcategory

    return "General"



def extract_regex_from_subject(subject, headline):

    """Extract subcategory from announcement title/headline."""

    if not subject and not headline:

        return None

        

    text = subject or headline

    

    # The exact pattern in your screenshot example

    lodr_pattern = r"(?:Regulation\s+30|LODR)[-\s]*[Aa]cquisition"

    if re.search(lodr_pattern, text, re.IGNORECASE):

        return "Acquisition"

    

    # More general pattern for LODR announcements

    patterns = [

        r"(?:Regulation\s+30|LODR).*?[-:]\s*(.*?)(?:\s*$|\s*\.)",

        r"(?:^|\s)([A-Za-z\s]+)(?:\s*$|\s*\.)",

    ]

    

    for pattern in patterns:

        match = re.search(pattern, text, re.IGNORECASE)

        if match:

            subcat = match.group(1).strip()

            if subcat and len(subcat) < 30:  # Avoid capturing too much text

                return subcat

    

    return None





def main():

    try:

        logging.info("Fetching BSE announcements...")

        data = bse.announcements()

        

        # Handle empty or error responses

        if not data or 'Table' not in data:

            logging.error(f"Invalid API response: {data}")

            return

            

        table_data = data.get('Table', [])

        if isinstance(table_data, dict):

            table_data = [table_data]

            

        # Convert to DataFrame

        df = pd.DataFrame(table_data)

        

        if df.empty:

            logging.info("No announcements found")

            return

            

        # Process dates

        df['NEWS_DT'] = pd.to_datetime(df['NEWS_DT'], errors='coerce')

        today = datetime.today().date()

        df_today = df[df['NEWS_DT'].dt.date == today]

        

        logging.info(f"Found {len(df_today)} announcements for today")

        

        # Extract additional data

        final_data = []

        for _, row in df_today.iterrows():

            stock_code = str(row.get('SCRIP_CD', '')).strip()

            stock_name = row.get('SLONGNAME', '').strip() or row.get('SSHORTNAME', '').strip()

            headline = row.get('HEADLINE') or row.get('NEWSSUB') or 'No headline'

            news_date = row['NEWS_DT'].strftime('%d/%m/%y') if pd.notnull(row['NEWS_DT']) else ''

            news_id = row.get('NEWSID')

            

            if not news_id:

                continue

                

            # Add rate limiting to avoid being blocked

            time.sleep(0.5)

            

            logging.info(f"Processing NEWSID: {news_id}")

            pdf_link, category, subcategory = get_pdf_and_categories(news_id)

            

            # Try to extract subcategory from headline if not found

            if not subcategory or subcategory == "General":

                extracted_subcat = extract_regex_from_subject(None, headline)

                if extracted_subcat:

                    subcategory = map_subcategory(extracted_subcat)

            

            final_data.append({

                "Stock Code": stock_code,

                "Stock Name": stock_name,

                "Headline": headline.strip(),

                "Main Category": category.strip(),

                "Subcategory": subcategory.strip(),

                "PDF Link": pdf_link,

                "Date": news_date,

                "News ID": news_id

            })

            

        # Save and display results

        df_final = pd.DataFrame(final_data)

        output_file = f"bse_announcements_{today.strftime('%Y%m%d')}.csv"

        df_final.to_csv(output_file, index=False)

        logging.info(f"Saved announcements to {output_file}")

        

        # Display sample results

        print(df_final.head())

        

    except Exception as e:

        logging.error(f"An error occurred: {e}")



if __name__ == "__main__":

    main()