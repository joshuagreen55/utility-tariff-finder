"""
Address Lookup Test Script

Tests the address lookup API with 10 real addresses per US state / Canadian
province and outputs results to CSV and a summary table.

Usage:
    python -m scripts.test_address_lookup --country US [--base-url http://localhost:8000]
    python -m scripts.test_address_lookup --country CA
    python -m scripts.test_address_lookup --country both
"""

import argparse
import csv
import time
from pathlib import Path

import httpx

# ---------------------------------------------------------------------------
# Canadian addresses (13 provinces/territories × 10)
# ---------------------------------------------------------------------------
CA_ADDRESSES: dict[str, list[str]] = {
    "AB": [
        "9820 106 St NW, Edmonton, AB T5K 1C6, Canada",
        "800 Macleod Trail SE, Calgary, AB T2G 2M3, Canada",
        "4700 49 Ave, Red Deer, AB T4N 6L2, Canada",
        "100 World Festival Trail, Lethbridge, AB T1J 4C7, Canada",
        "9909 Franklin Ave, Fort McMurray, AB T9H 2K4, Canada",
        "10111 104 Ave, Grande Prairie, AB T8V 0Y6, Canada",
        "10 Akins Dr, St. Albert, AB T8N 3Z1, Canada",
        "401 1 Ave SE, Medicine Hat, AB T1A 8E4, Canada",
        "200 Karl Clark Rd NW, Edmonton, AB T6N 1H2, Canada",
        "510 12 Ave S, Lethbridge, AB T1J 0R4, Canada",
    ],
    "BC": [
        "453 W 12th Ave, Vancouver, BC V5Y 1V4, Canada",
        "655 Douglas St, Victoria, BC V8V 2P9, Canada",
        "1221 Canyon Blvd, North Vancouver, BC V7J 2J1, Canada",
        "1370 Dominion Ave, Kelowna, BC V1Y 6H2, Canada",
        "250 8th St, Prince George, BC V2L 5L7, Canada",
        "7380 137 St, Surrey, BC V3W 1A3, Canada",
        "550 Victoria St, Kamloops, BC V2C 2B2, Canada",
        "3001 Wayburne Dr, Burnaby, BC V5G 4W3, Canada",
        "101 Nicol St, Nanaimo, BC V9R 4S3, Canada",
        "100 27th St E, Prince Albert, BC V1B 1T6, Canada",
    ],
    "MB": [
        "510 Main St, Winnipeg, MB R3B 1B9, Canada",
        "1 Wesley Ave, Winnipeg, MB R3C 4C6, Canada",
        "25 Forks Market Rd, Winnipeg, MB R3C 4S8, Canada",
        "59 Elizabeth Dr, Thompson, MB R8N 1X4, Canada",
        "1120 Victoria Ave E, Brandon, MB R7A 2A9, Canada",
        "100 9th St, Brandon, MB R7A 6C2, Canada",
        "20 Main St S, Dauphin, MB R7N 1K3, Canada",
        "236 Saskatchewan Ave E, Portage la Prairie, MB R1N 0K6, Canada",
        "184 Main St, Selkirk, MB R1A 1R2, Canada",
        "421 Park Ave E, Steinbach, MB R5G 1G3, Canada",
    ],
    "NB": [
        "770 Main St, Moncton, NB E1C 1E8, Canada",
        "1 Market Square, Saint John, NB E2L 4Z6, Canada",
        "12 Smythe St, Fredericton, NB E3B 3E3, Canada",
        "215 King St, Bathurst, NB E2A 1L7, Canada",
        "100 Arden St, Miramichi, NB E1V 3G7, Canada",
        "120 Harbourview Blvd, Dieppe, NB E1A 6K7, Canada",
        "95 Foundry St, Moncton, NB E1C 5H7, Canada",
        "300 St. George St, Moncton, NB E1C 1W7, Canada",
        "625 Beaverbrook Ct, Fredericton, NB E3B 5X4, Canada",
        "75 Milltown Blvd, St. Stephen, NB E3L 1G5, Canada",
    ],
    "NL": [
        "46A Quidi Vidi Village Rd, St. John's, NL A1A 0R1, Canada",
        "245 Freshwater Rd, St. John's, NL A1B 1B9, Canada",
        "2 Herald Ave, Corner Brook, NL A2H 4B4, Canada",
        "214 Main Rd, Grand Falls-Windsor, NL A2A 1J8, Canada",
        "10 Elizabeth Dr, Paradise, NL A1L 0B1, Canada",
        "100 New Gower St, St. John's, NL A1C 6K3, Canada",
        "1 Crosbie Pl, St. John's, NL A1B 3Y8, Canada",
        "275 Magee Rd, Gander, NL A1V 1V1, Canada",
        "460 Torbay Rd, St. John's, NL A1A 5J3, Canada",
        "87 Conception Bay Hwy, Conception Bay South, NL A1W 3A5, Canada",
    ],
    "NS": [
        "51 Roxbury Crescent, Halifax, NS B3M 4S9, Canada",
        "5151 Terminal Rd, Halifax, NS B3J 1A1, Canada",
        "5440 Spring Garden Rd, Halifax, NS B3J 1E9, Canada",
        "275 Foord St, Stellarton, NS B0K 1S0, Canada",
        "80 Aberdeen St, Truro, NS B2N 1K6, Canada",
        "295 Charlotte St, Sydney, NS B1P 1C5, Canada",
        "800 East River Rd, New Glasgow, NS B2H 3S8, Canada",
        "300 Prince St, Dartmouth, NS B2Y 4K2, Canada",
        "117 King St, Bridgewater, NS B4V 1B3, Canada",
        "126 Commercial St, Glace Bay, NS B1A 3B9, Canada",
    ],
    "NT": [
        "4807 49th St, Yellowknife, NT X1A 3T5, Canada",
        "5022 49th St, Yellowknife, NT X1A 1P8, Canada",
        "4920 52 St, Yellowknife, NT X1A 3T1, Canada",
        "4501 Franklin Ave, Yellowknife, NT X1A 2N1, Canada",
        "5004 54 St, Yellowknife, NT X1A 2R6, Canada",
        "100 Bison Rd, Hay River, NT X0E 0R6, Canada",
        "171 Mackenzie Rd, Inuvik, NT X0E 0T0, Canada",
        "1 Studge Dr, Hay River, NT X0E 0R5, Canada",
        "48 Calder Pl, Yellowknife, NT X1A 2B4, Canada",
        "5201 50 Ave, Yellowknife, NT X1A 1E2, Canada",
    ],
    "NU": [
        "630 Queen Elizabeth Way, Iqaluit, NU X0A 0H0, Canada",
        "926 Federal Rd, Iqaluit, NU X0A 0H0, Canada",
        "1085 Mivvik St, Iqaluit, NU X0A 0H0, Canada",
        "584 Niaqunngut Trail, Iqaluit, NU X0A 0H0, Canada",
        "200 Sinaa St, Iqaluit, NU X0A 0H0, Canada",
        "Hamlet of Rankin Inlet, Rankin Inlet, NU X0C 0G0, Canada",
        "100 Tumiit Plaza, Rankin Inlet, NU X0C 0G0, Canada",
        "Hamlet of Cambridge Bay, Cambridge Bay, NU X0B 0C0, Canada",
        "1 Kuuvik St, Arviat, NU X0C 0E0, Canada",
        "Hamlet of Baker Lake, Baker Lake, NU X0C 0A0, Canada",
    ],
    "ON": [
        "100 Queen St W, Toronto, ON M5H 2N2, Canada",
        "1 Nicholas St, Ottawa, ON K1N 7B7, Canada",
        "1 Dundas St W, Mississauga, ON L5B 1H7, Canada",
        "71 Main St W, Hamilton, ON L8P 4Y5, Canada",
        "150 Frederick St, Kitchener, ON N2G 4X3, Canada",
        "380 Wellington St, London, ON N6A 5B5, Canada",
        "400 Ouellette Ave, Windsor, ON N9A 7B3, Canada",
        "216 First Ave S, Thunder Bay, ON P7B 5G4, Canada",
        "160 King St W, Oshawa, ON L1J 2K3, Canada",
        "1 Johnson St, Kingston, ON K7L 1X7, Canada",
    ],
    "PE": [
        "175 Richmond St, Charlottetown, PE C1A 1H7, Canada",
        "1 Harbourside Access Rd, Charlottetown, PE C1A 8R4, Canada",
        "550 University Ave, Charlottetown, PE C1A 4P3, Canada",
        "670 University Ave, Charlottetown, PE C1A 4P3, Canada",
        "64 Fitzroy St, Charlottetown, PE C1A 1R4, Canada",
        "250 Water St, Summerside, PE C1N 1B6, Canada",
        "100 Central St, Summerside, PE C1N 3L2, Canada",
        "17798 Trans-Canada Hwy, Cornwall, PE C0A 1H0, Canada",
        "430 Notre Dame St, Summerside, PE C1N 1J6, Canada",
        "12 Kent St, Charlottetown, PE C1A 1L2, Canada",
    ],
    "QC": [
        "275 Rue Notre-Dame E, Montréal, QC H2Y 1C6, Canada",
        "1000 Rue De La Gauchetière O, Montréal, QC H3B 4W5, Canada",
        "1001 Place Jean-Paul-Riopelle, Montréal, QC H2Z 1H5, Canada",
        "900 Boul René-Lévesque E, Québec, QC G1R 2B5, Canada",
        "300 Allée des Ursulines, Trois-Rivières, QC G9A 5B9, Canada",
        "200 Rue King O, Sherbrooke, QC J1H 1P8, Canada",
        "855 Rue De La Concorde, Lévis, QC G6W 7P5, Canada",
        "1 Rue du Vieux-Port, Gatineau, QC J8X 4B7, Canada",
        "51 Boul St-Raymond, Gatineau, QC J8Y 1R8, Canada",
        "2900 Boul Laurier, Québec, QC G1V 2M2, Canada",
    ],
    "SK": [
        "2903 Powerhouse Dr, Regina, SK S4N 0A1, Canada",
        "2102 11th Ave, Regina, SK S4P 3Z8, Canada",
        "311 21st St E, Saskatoon, SK S7K 0C1, Canada",
        "1102 8th Ave, Regina, SK S4R 1C9, Canada",
        "601 Spadina Crescent E, Saskatoon, SK S7K 3G8, Canada",
        "45 23rd St E, Prince Albert, SK S6V 1R8, Canada",
        "119 4th Ave S, Saskatoon, SK S7K 5X2, Canada",
        "1410 20th St W, Saskatoon, SK S7M 0Z4, Canada",
        "350 Broad St, Regina, SK S4R 1X2, Canada",
        "3130 8th St E, Saskatoon, SK S7H 0W2, Canada",
    ],
    "YT": [
        "2071 2nd Ave, Whitehorse, YT Y1A 1B5, Canada",
        "4250 4th Ave, Whitehorse, YT Y1A 1K1, Canada",
        "1171 Front St, Whitehorse, YT Y1A 0G9, Canada",
        "100 Hanson St, Whitehorse, YT Y1A 1Y3, Canada",
        "2190 2nd Ave, Whitehorse, YT Y1A 3T8, Canada",
        "302 Steele St, Whitehorse, YT Y1A 2C5, Canada",
        "405 Alexander St, Whitehorse, YT Y1A 1L9, Canada",
        "600 College Dr, Whitehorse, YT Y1A 5K4, Canada",
        "3126 3rd Ave, Whitehorse, YT Y1A 1E7, Canada",
        "1000 Lewes Blvd, Whitehorse, YT Y1A 3H7, Canada",
    ],
}

CA_REGIONS = [
    "AB", "BC", "MB", "NB", "NL", "NS", "NT", "NU",
    "ON", "PE", "QC", "SK", "YT",
]

# ---------------------------------------------------------------------------
# US addresses (50 states + DC × 10)
# ---------------------------------------------------------------------------
US_ADDRESSES: dict[str, list[str]] = {
    "AL": [
        "600 Dexter Ave, Montgomery, AL 36104",
        "1 Tranquility Base, Huntsville, AL 35805",
        "1815 Rev Abraham Woods Jr Blvd, Birmingham, AL 35203",
        "2700 Airport Blvd, Mobile, AL 36606",
        "401 Adams Ave, Montgomery, AL 36104",
        "3100 Leeman Ferry Rd, Huntsville, AL 35801",
        "420 20th St N, Birmingham, AL 35203",
        "564 Bel Air Blvd, Mobile, AL 36606",
        "500 Gault Ave N, Fort Payne, AL 35967",
        "700 Monroe St, Montgomery, AL 36104",
    ],
    "AK": [
        "600 W 4th Ave, Anchorage, AK 99501",
        "100 Cushman St, Fairbanks, AK 99701",
        "400 Willoughby Ave, Juneau, AK 99801",
        "2309 Spenard Rd, Anchorage, AK 99503",
        "3601 C St, Anchorage, AK 99503",
        "525 W 3rd Ave, Anchorage, AK 99501",
        "1025 Egan Dr, Juneau, AK 99801",
        "501 Railway Ave, Seward, AK 99664",
        "1000 University Ave, Fairbanks, AK 99709",
        "209 4th Ave, Skagway, AK 99840",
    ],
    "AZ": [
        "1700 W Washington St, Phoenix, AZ 85007",
        "150 N Stone Ave, Tucson, AZ 85701",
        "1 E Washington St, Phoenix, AZ 85004",
        "20 E Main St, Mesa, AZ 85201",
        "6840 E 2nd St, Scottsdale, AZ 85251",
        "100 W Birch Ave, Flagstaff, AZ 86001",
        "7000 E Mayo Blvd, Phoenix, AZ 85054",
        "2621 N Country Club Rd, Tucson, AZ 85716",
        "3300 N Dysart Rd, Avondale, AZ 85392",
        "9301 E Shea Blvd, Scottsdale, AZ 85260",
    ],
    "AR": [
        "500 President Clinton Ave, Little Rock, AR 72201",
        "100 E 8th St, Little Rock, AR 72201",
        "2503 Central Ave, Hot Springs, AR 71901",
        "101 E Markham St, Little Rock, AR 72201",
        "3000 Kavanaugh Blvd, Little Rock, AR 72205",
        "100 S Main St, Jonesboro, AR 72401",
        "125 W Mountain St, Fayetteville, AR 72701",
        "200 SE A St, Bentonville, AR 72712",
        "401 E Markham St, Little Rock, AR 72201",
        "600 Main St, Pine Bluff, AR 71601",
    ],
    "CA": [
        "200 N Spring St, Los Angeles, CA 90012",
        "1 Dr Carlton B Goodlett Pl, San Francisco, CA 94102",
        "202 C St, San Diego, CA 92101",
        "300 Lakeside Dr, Oakland, CA 94612",
        "1 Capitol Mall, Sacramento, CA 95814",
        "100 W Broadway, Long Beach, CA 90802",
        "600 E St, Fresno, CA 93721",
        "175 5th Ave, San Jose, CA 95113",
        "100 N Garfield Ave, Pasadena, CA 91101",
        "250 Hamilton Ave, Palo Alto, CA 94301",
    ],
    "CO": [
        "200 W Colfax Ave, Denver, CO 80202",
        "107 N Nevada Ave, Colorado Springs, CO 80903",
        "300 LaPorte Ave, Fort Collins, CO 80521",
        "1001 11th Ave, Greeley, CO 80631",
        "500 E 3rd St, Pueblo, CO 81001",
        "511 Colorado Ave, Grand Junction, CO 81501",
        "1777 Broadway, Boulder, CO 80302",
        "150 W 9th St, Durango, CO 81301",
        "100 Centennial Blvd, Highlands Ranch, CO 80129",
        "3001 S Federal Blvd, Denver, CO 80236",
    ],
    "CT": [
        "165 Capitol Ave, Hartford, CT 06106",
        "200 Orange St, New Haven, CT 06510",
        "888 Washington Blvd, Stamford, CT 06901",
        "140 Main St, Bridgeport, CT 06604",
        "71 Elm St, Hartford, CT 06106",
        "1000 Lafayette Blvd, Bridgeport, CT 06604",
        "110 Elm St, New London, CT 06320",
        "400 Main St, Danbury, CT 06810",
        "110 Charlotte St, Hartford, CT 06106",
        "250 Constitution Plaza, Hartford, CT 06103",
    ],
    "DE": [
        "411 Legislative Ave, Dover, DE 19901",
        "800 N French St, Wilmington, DE 19801",
        "1 Customs House Sq, Wilmington, DE 19801",
        "100 W 10th St, Wilmington, DE 19801",
        "818 N Market St, Wilmington, DE 19801",
        "520 N Market St, Wilmington, DE 19801",
        "630 Churchmans Rd, Newark, DE 19702",
        "200 Commerce Way, Dover, DE 19904",
        "46 The Green, Dover, DE 19901",
        "83 E Main St, Newark, DE 19711",
    ],
    "DC": [
        "1600 Pennsylvania Ave NW, Washington, DC 20500",
        "1 First St NE, Washington, DC 20543",
        "500 12th St SW, Washington, DC 20024",
        "1350 Pennsylvania Ave NW, Washington, DC 20004",
        "2 Lincoln Memorial Cir NW, Washington, DC 20037",
        "700 14th St NW, Washington, DC 20005",
        "555 Pennsylvania Ave NW, Washington, DC 20001",
        "1100 4th St SW, Washington, DC 20024",
        "3101 Wisconsin Ave NW, Washington, DC 20016",
        "801 Mount Vernon Pl NW, Washington, DC 20001",
    ],
    "FL": [
        "400 S Monroe St, Tallahassee, FL 32399",
        "300 Biscayne Blvd Way, Miami, FL 33131",
        "400 E Jackson St, Tampa, FL 33602",
        "300 N Beach St, Daytona Beach, FL 32114",
        "151 SW 2nd Ave, Fort Lauderdale, FL 33301",
        "100 N Laura St, Jacksonville, FL 32202",
        "195 Central Ave, St. Petersburg, FL 33701",
        "401 E Pine St, Orlando, FL 32801",
        "200 NW 1st Ave, Gainesville, FL 32601",
        "300 S Tamiami Trail, Venice, FL 34285",
    ],
    "GA": [
        "206 Washington St SW, Atlanta, GA 30334",
        "200 E Bay St, Savannah, GA 31401",
        "100 10th St NW, Atlanta, GA 30309",
        "601 Broad St, Augusta, GA 30901",
        "200 Cherry St, Macon, GA 31201",
        "300 Mulberry St, Macon, GA 31201",
        "100 N Main St, Jonesboro, GA 30236",
        "100 S Milledge Ave, Athens, GA 30605",
        "175 Piedmont Ave NE, Atlanta, GA 30303",
        "300 W Broad Ave, Albany, GA 31701",
    ],
    "HI": [
        "415 S Beretania St, Honolulu, HI 96813",
        "100 Holomoana St, Honolulu, HI 96815",
        "200 Ward Ave, Honolulu, HI 96814",
        "2600 Campus Rd, Honolulu, HI 96822",
        "1 Aloha Tower Dr, Honolulu, HI 96813",
        "250 Waianuenue Ave, Hilo, HI 96720",
        "2500 Kuhio Ave, Honolulu, HI 96815",
        "4303 Diamond Head Rd, Honolulu, HI 96816",
        "2335 Kalakaua Ave, Honolulu, HI 96815",
        "3950 Lawehana St, Lihue, HI 96766",
    ],
    "ID": [
        "700 W Jefferson St, Boise, ID 83702",
        "250 S 5th Ave, Pocatello, ID 83201",
        "911 N 8th St, Boise, ID 83702",
        "350 N 9th St, Boise, ID 83702",
        "415 N Arthur St, Pocatello, ID 83204",
        "100 N Main St, Moscow, ID 83843",
        "200 E Riverside Ave, Ketchum, ID 83340",
        "380 Memorial Dr, Idaho Falls, ID 83402",
        "720 W Idaho St, Boise, ID 83702",
        "1100 W Iron Springs Rd, Prescott, ID 86305",
    ],
    "IL": [
        "100 W Randolph St, Chicago, IL 60601",
        "1 N Old State Capitol Plaza, Springfield, IL 62701",
        "401 N Main St, Rockford, IL 61101",
        "100 E University Ave, Champaign, IL 61820",
        "625 E Adams St, Springfield, IL 62701",
        "200 S Wacker Dr, Chicago, IL 60606",
        "230 S LaSalle St, Chicago, IL 60604",
        "500 S 2nd St, Springfield, IL 62701",
        "1 Millennium Park Plaza, Chicago, IL 60602",
        "150 N Wacker Dr, Chicago, IL 60606",
    ],
    "IN": [
        "200 W Washington St, Indianapolis, IN 46204",
        "1 Civic Square, Hammond, IN 46324",
        "101 W Ohio St, Indianapolis, IN 46204",
        "1 Main St, Evansville, IN 47708",
        "300 E Main St, Fort Wayne, IN 46802",
        "301 S Capitol Ave, Indianapolis, IN 46225",
        "250 E Market St, Indianapolis, IN 46204",
        "1 N Capitol Ave, Indianapolis, IN 46204",
        "500 S Capitol Ave, Indianapolis, IN 46225",
        "100 S Main St, South Bend, IN 46601",
    ],
    "IA": [
        "1007 E Grand Ave, Des Moines, IA 50319",
        "50 2nd Ave Bridge, Cedar Rapids, IA 52401",
        "100 State St, Des Moines, IA 50309",
        "600 Walnut St, Des Moines, IA 50309",
        "122 S Linn St, Iowa City, IA 52240",
        "500 Bluff St, Dubuque, IA 52001",
        "100 1st St NE, Cedar Rapids, IA 52401",
        "401 Douglas St, Sioux City, IA 51101",
        "800 Lincoln Way, Ames, IA 50010",
        "100 Central Ave NW, Le Mars, IA 51031",
    ],
    "KS": [
        "300 SW 10th Ave, Topeka, KS 66612",
        "455 N Main St, Wichita, KS 67202",
        "701 N 7th St, Kansas City, KS 66101",
        "100 E 9th St, Lawrence, KS 66044",
        "201 N Water St, Wichita, KS 67202",
        "150 S Santa Fe Ave, Salina, KS 67401",
        "200 E Poplar St, Olathe, KS 66061",
        "100 N Broadway Blvd, Wichita, KS 67202",
        "330 E William St, Wichita, KS 67202",
        "515 S Kansas Ave, Topeka, KS 66603",
    ],
    "KY": [
        "700 Capitol Ave, Frankfort, KY 40601",
        "601 W Main St, Louisville, KY 40202",
        "100 W Vine St, Lexington, KY 40507",
        "1 Riverfront Plaza, Louisville, KY 40202",
        "401 S 4th St, Louisville, KY 40202",
        "215 W Main St, Frankfort, KY 40601",
        "200 E Main St, Lexington, KY 40507",
        "999 Expo Dr, Bowling Green, KY 42101",
        "1 Quality St, Lexington, KY 40507",
        "140 N Broadway, Lexington, KY 40507",
    ],
    "LA": [
        "900 N 3rd St, Baton Rouge, LA 70802",
        "1300 Perdido St, New Orleans, LA 70112",
        "505 Travis St, Shreveport, LA 71101",
        "1515 Poydras St, New Orleans, LA 70112",
        "400 Texas St, Shreveport, LA 71101",
        "100 Lafayette St, Baton Rouge, LA 70801",
        "400 Poydras St, New Orleans, LA 70130",
        "601 Ryan St, Lake Charles, LA 70601",
        "800 Main St, Monroe, LA 71201",
        "701 Loyola Ave, New Orleans, LA 70113",
    ],
    "ME": [
        "1 State House Station, Augusta, ME 04333",
        "389 Congress St, Portland, ME 04101",
        "1 City Center, Portland, ME 04101",
        "168 Middle St, Portland, ME 04101",
        "100 Community Dr, Augusta, ME 04330",
        "27 State St, Bangor, ME 04401",
        "73 Main St, Bar Harbor, ME 04609",
        "62 Main St, Camden, ME 04843",
        "1 City Hall Sq, Lewiston, ME 04240",
        "100 Hogan Rd, Bangor, ME 04401",
    ],
    "MD": [
        "100 State Circle, Annapolis, MD 21401",
        "100 N Holliday St, Baltimore, MD 21202",
        "111 S Calvert St, Baltimore, MD 21202",
        "250 W Pratt St, Baltimore, MD 21201",
        "7500 Greenway Center Dr, Greenbelt, MD 20770",
        "50 Harry S Truman Pkwy, Annapolis, MD 21401",
        "1 Olympic Pl, Gaithersburg, MD 20877",
        "700 E Pratt St, Baltimore, MD 21202",
        "100 Edison Park Dr, Gaithersburg, MD 20878",
        "6301 Ivy Ln, Greenbelt, MD 20770",
    ],
    "MA": [
        "24 Beacon St, Boston, MA 02133",
        "1 City Hall Sq, Boston, MA 02201",
        "200 Clarendon St, Boston, MA 02116",
        "1 Kendall Sq, Cambridge, MA 02139",
        "36 Court St, Springfield, MA 01103",
        "455 Main St, Worcester, MA 01608",
        "1 Federal St, Boston, MA 02110",
        "100 Summer St, Boston, MA 02110",
        "77 Massachusetts Ave, Cambridge, MA 02139",
        "1 Beacon St, Boston, MA 02108",
    ],
    "MI": [
        "100 State Capitol, Lansing, MI 48933",
        "2 Woodward Ave, Detroit, MI 48226",
        "100 Monroe Center St NW, Grand Rapids, MI 49503",
        "300 S Washington Sq, Lansing, MI 48933",
        "1001 Woodward Ave, Detroit, MI 48226",
        "825 Washington Ave SE, Grand Rapids, MI 49507",
        "500 S State St, Ann Arbor, MI 48109",
        "100 E Michigan Ave, Kalamazoo, MI 49007",
        "315 E Eisenhower Pkwy, Ann Arbor, MI 48108",
        "301 E Michigan Ave, Lansing, MI 48933",
    ],
    "MN": [
        "75 Rev Dr Martin Luther King Jr Blvd, St. Paul, MN 55155",
        "350 S 5th St, Minneapolis, MN 55415",
        "400 Robert St N, St. Paul, MN 55101",
        "90 W Plato Blvd, St. Paul, MN 55107",
        "101 E 5th St, Duluth, MN 55802",
        "250 2nd Ave S, Minneapolis, MN 55401",
        "328 W Superior St, Duluth, MN 55802",
        "300 Nicollet Mall, Minneapolis, MN 55401",
        "100 S Robert St, St. Paul, MN 55107",
        "501 Marquette Ave, Minneapolis, MN 55402",
    ],
    "MS": [
        "400 High St, Jackson, MS 39201",
        "200 S Lamar St, Jackson, MS 39201",
        "300 E Pearl St, Jackson, MS 39201",
        "1 Convention Center Plaza, Gulfport, MS 39501",
        "710 Front St, Hattiesburg, MS 39401",
        "300 E Beach Blvd, Gulfport, MS 39507",
        "308 23rd Ave, Meridian, MS 39301",
        "100 S State St, Jackson, MS 39201",
        "1 Resort Dr, Tunica, MS 38676",
        "300 W Beach Blvd, Biloxi, MS 39530",
    ],
    "MO": [
        "201 W Capitol Ave, Jefferson City, MO 65101",
        "1200 Market St, St. Louis, MO 63103",
        "414 E 12th St, Kansas City, MO 64106",
        "100 N Broadway, St. Louis, MO 63102",
        "901 N 10th St, St. Louis, MO 63101",
        "1 Metropolitan Sq, St. Louis, MO 63102",
        "200 E 12th St, Kansas City, MO 64106",
        "100 N Main Ave, Springfield, MO 65806",
        "400 Main St, Kansas City, MO 64105",
        "601 N Grand Blvd, St. Louis, MO 63103",
    ],
    "MT": [
        "1301 E 6th Ave, Helena, MT 59601",
        "710 S Atlantic St, Missoula, MT 59801",
        "100 W Broadway, Butte, MT 59701",
        "2950 Expo Pkwy, Missoula, MT 59808",
        "200 W Pine St, Missoula, MT 59802",
        "300 Central Ave, Great Falls, MT 59401",
        "100 Railroad St, Whitefish, MT 59937",
        "101 E Main St, Bozeman, MT 59715",
        "401 N Last Chance Gulch, Helena, MT 59601",
        "316 N 26th St, Billings, MT 59101",
    ],
    "NE": [
        "1445 K St, Lincoln, NE 68508",
        "1819 Farnam St, Omaha, NE 68183",
        "300 S 18th St, Omaha, NE 68102",
        "555 S 10th St, Lincoln, NE 68508",
        "100 Centennial Mall N, Lincoln, NE 68508",
        "225 Regency Pkwy, Omaha, NE 68114",
        "100 N Jeffers St, North Platte, NE 69101",
        "1001 Farnam St, Omaha, NE 68102",
        "300 Canfield Ave, Grand Island, NE 68801",
        "1500 S 48th St, Lincoln, NE 68506",
    ],
    "NV": [
        "101 N Carson St, Carson City, NV 89701",
        "495 S Main St, Las Vegas, NV 89101",
        "1 E 1st St, Reno, NV 89501",
        "3150 Paradise Rd, Las Vegas, NV 89109",
        "3600 S Las Vegas Blvd, Las Vegas, NV 89109",
        "200 S Virginia St, Reno, NV 89501",
        "100 Stewart St, Carson City, NV 89701",
        "500 S Grand Central Pkwy, Las Vegas, NV 89106",
        "100 N Sierra St, Reno, NV 89501",
        "3799 S Las Vegas Blvd, Las Vegas, NV 89109",
    ],
    "NH": [
        "107 N Main St, Concord, NH 03301",
        "900 Elm St, Manchester, NH 03101",
        "1 City Hall Plaza, Manchester, NH 03101",
        "1 Eagle Sq, Concord, NH 03301",
        "100 Main St, Nashua, NH 03060",
        "2 Pillsbury St, Concord, NH 03301",
        "100 Market St, Portsmouth, NH 03801",
        "500 Commercial St, Manchester, NH 03101",
        "100 Congress St, Portsmouth, NH 03801",
        "100 N Main St, Plymouth, NH 03264",
    ],
    "NJ": [
        "125 W State St, Trenton, NJ 08608",
        "280 Grove St, Jersey City, NJ 07302",
        "920 Broad St, Newark, NJ 07102",
        "1 Penn Plaza E, Newark, NJ 07105",
        "100 Boardwalk, Atlantic City, NJ 08401",
        "1 Exchange Pl, Jersey City, NJ 07302",
        "200 Federal St, Camden, NJ 08103",
        "1 Riverfront Plaza, Newark, NJ 07102",
        "100 Campus Town Cir, Ewing, NJ 08628",
        "500 Route 73 S, Marlton, NJ 08053",
    ],
    "NM": [
        "490 Old Santa Fe Trail, Santa Fe, NM 87501",
        "1 Civic Plaza NW, Albuquerque, NM 87102",
        "200 E Broadway, Farmington, NM 87401",
        "700 N Main St, Las Cruces, NM 88001",
        "100 Sun Ave NE, Albuquerque, NM 87109",
        "100 Gold Ave SW, Albuquerque, NM 87102",
        "500 4th St NW, Albuquerque, NM 87102",
        "300 N Downtown Mall, Las Cruces, NM 88001",
        "120 S Federal Pl, Santa Fe, NM 87501",
        "400 Marquette Ave NW, Albuquerque, NM 87102",
    ],
    "NY": [
        "350 Fifth Ave, New York, NY 10118",
        "1 Police Plaza, New York, NY 10038",
        "200 Eastern Pkwy, Brooklyn, NY 11238",
        "City Hall, Albany, NY 12207",
        "1 Niagara Sq, Buffalo, NY 14202",
        "30 Church St, Rochester, NY 14614",
        "233 N Geneva St, Ithaca, NY 14850",
        "100 State St, Albany, NY 12207",
        "200 Washington Ave, Albany, NY 12210",
        "2 Lafayette St, New York, NY 10007",
    ],
    "NC": [
        "1 E Edenton St, Raleigh, NC 27601",
        "100 N Tryon St, Charlotte, NC 28202",
        "300 N Greene St, Greensboro, NC 27401",
        "600 Fayetteville St, Raleigh, NC 27601",
        "1 W Pack Sq, Asheville, NC 28801",
        "101 N Main St, Winston-Salem, NC 27101",
        "201 S Eugene St, Greensboro, NC 27401",
        "200 N College St, Charlotte, NC 28202",
        "310 New Bern Ave, Raleigh, NC 27601",
        "100 Patton Ave, Asheville, NC 28801",
    ],
    "ND": [
        "600 E Boulevard Ave, Bismarck, ND 58505",
        "225 N 5th St, Fargo, ND 58102",
        "15 N 3rd St, Grand Forks, ND 58203",
        "100 E Broadway Ave, Bismarck, ND 58501",
        "200 3rd St N, Fargo, ND 58102",
        "300 DeMers Ave, Grand Forks, ND 58201",
        "100 28th St S, Fargo, ND 58103",
        "500 2nd Ave N, Fargo, ND 58102",
        "400 E Main Ave, Bismarck, ND 58501",
        "110 N 3rd St, Bismarck, ND 58501",
    ],
    "OH": [
        "1 Capitol Sq, Columbus, OH 43215",
        "601 Lakeside Ave, Cleveland, OH 44114",
        "801 Plum St, Cincinnati, OH 45202",
        "1 Gov Center, Toledo, OH 43604",
        "166 S High St, Columbus, OH 43215",
        "200 W 2nd St, Dayton, OH 45402",
        "1 Cascade Plaza, Akron, OH 44308",
        "106 S Main St, Akron, OH 44308",
        "120 E 4th St, Cincinnati, OH 45202",
        "100 E Broad St, Columbus, OH 43215",
    ],
    "OK": [
        "2300 N Lincoln Blvd, Oklahoma City, OK 73105",
        "175 E 2nd St, Tulsa, OK 74103",
        "200 N Walker Ave, Oklahoma City, OK 73102",
        "100 E Eufaula St, Norman, OK 73069",
        "321 S Boston Ave, Tulsa, OK 74103",
        "100 N Broadway Ave, Oklahoma City, OK 73102",
        "320 S Boston Ave, Tulsa, OK 74103",
        "400 Civic Center Dr, Oklahoma City, OK 73102",
        "301 W Boyd St, Norman, OK 73069",
        "111 W 5th St, Tulsa, OK 74103",
    ],
    "OR": [
        "900 Court St NE, Salem, OR 97301",
        "1221 SW 4th Ave, Portland, OR 97204",
        "100 W 8th Ave, Eugene, OR 97401",
        "940 Willamette St, Eugene, OR 97401",
        "125 E 8th Ave, Eugene, OR 97401",
        "200 SW Market St, Portland, OR 97201",
        "800 NE Oregon St, Portland, OR 97232",
        "100 S Central Ave, Medford, OR 97501",
        "500 Liberty St SE, Salem, OR 97301",
        "200 NW 5th St, Corvallis, OR 97330",
    ],
    "PA": [
        "501 N 3rd St, Harrisburg, PA 17120",
        "1 Penn Sq, Philadelphia, PA 19107",
        "400 Grant St, Pittsburgh, PA 15219",
        "200 S Broad St, Philadelphia, PA 19102",
        "100 N Queen St, Lancaster, PA 17603",
        "530 Walnut St, Philadelphia, PA 19106",
        "301 Grant St, Pittsburgh, PA 15219",
        "100 W Olney Ave, Philadelphia, PA 19120",
        "500 Penn St, Reading, PA 19601",
        "10 S Market Sq, Harrisburg, PA 17101",
    ],
    "RI": [
        "82 Smith St, Providence, RI 02903",
        "25 Dorrance St, Providence, RI 02903",
        "1 Sabin St, Providence, RI 02903",
        "100 Westminster St, Providence, RI 02903",
        "1 Financial Plaza, Providence, RI 02903",
        "222 Richmond St, Providence, RI 02903",
        "100 America's Cup Ave, Newport, RI 02840",
        "1 Avenue of the Arts, Providence, RI 02903",
        "100 Fountain St, Providence, RI 02903",
        "50 Memorial Blvd, Newport, RI 02840",
    ],
    "SC": [
        "1100 Gervais St, Columbia, SC 29201",
        "80 Broad St, Charleston, SC 29401",
        "100 E North St, Greenville, SC 29601",
        "206 N Main St, Greenville, SC 29601",
        "1 Broad St, Charleston, SC 29401",
        "1101 Main St, Columbia, SC 29201",
        "250 E Broad St, Greenville, SC 29601",
        "200 Meeting St, Charleston, SC 29401",
        "145 King St, Charleston, SC 29401",
        "300 S Evans St, Florence, SC 29506",
    ],
    "SD": [
        "500 E Capitol Ave, Pierre, SD 57501",
        "200 E 10th St, Sioux Falls, SD 57104",
        "444 W Mt Rushmore Rd, Rapid City, SD 57701",
        "100 N Phillips Ave, Sioux Falls, SD 57104",
        "300 N Dakota Ave, Sioux Falls, SD 57104",
        "523 6th St, Rapid City, SD 57701",
        "600 E Capitol Ave, Pierre, SD 57501",
        "601 S Main Ave, Sioux Falls, SD 57104",
        "200 St Joseph St, Rapid City, SD 57701",
        "411 E Capitol Ave, Pierre, SD 57501",
    ],
    "TN": [
        "600 Dr Martin Luther King Jr Blvd, Nashville, TN 37243",
        "125 N Main St, Memphis, TN 38103",
        "1 Market Sq, Knoxville, TN 37902",
        "1 Public Sq, Nashville, TN 37201",
        "100 N Main St, Memphis, TN 38103",
        "501 Broadway, Nashville, TN 37203",
        "500 James Robertson Pkwy, Nashville, TN 37243",
        "2600 Parkway, Pigeon Forge, TN 37863",
        "201 W Main St, Chattanooga, TN 37402",
        "600 S Gay St, Knoxville, TN 37902",
    ],
    "TX": [
        "1100 Congress Ave, Austin, TX 78701",
        "1500 Marilla St, Dallas, TX 75201",
        "901 Bagby St, Houston, TX 77002",
        "100 Military Plaza, San Antonio, TX 78205",
        "1000 Throckmorton St, Fort Worth, TX 76102",
        "2 Civic Center Plaza, El Paso, TX 79901",
        "1201 E Cesar Chavez St, Austin, TX 78702",
        "500 Main St, Fort Worth, TX 76102",
        "600 Commerce St, Dallas, TX 75202",
        "200 W Wall St, Midland, TX 79701",
    ],
    "UT": [
        "350 N State St, Salt Lake City, UT 84103",
        "451 S State St, Salt Lake City, UT 84111",
        "1 Arena Dr, Salt Lake City, UT 84101",
        "50 E North Temple, Salt Lake City, UT 84150",
        "100 S Main St, Provo, UT 84601",
        "174 N Main St, St. George, UT 84770",
        "2539 Washington Blvd, Ogden, UT 84401",
        "700 E University Pkwy, Provo, UT 84602",
        "300 S Rio Grande St, Salt Lake City, UT 84101",
        "200 E Center St, Moab, UT 84532",
    ],
    "VT": [
        "115 State St, Montpelier, VT 05633",
        "1 Church St, Burlington, VT 05401",
        "149 State St, Montpelier, VT 05602",
        "209 Battery St, Burlington, VT 05401",
        "255 S Champlain St, Burlington, VT 05401",
        "60 Main St, Burlington, VT 05401",
        "100 E State St, Montpelier, VT 05602",
        "111 West St, Rutland, VT 05701",
        "9 Central Ave, St. Johnsbury, VT 05819",
        "3 Park St, Middlebury, VT 05753",
    ],
    "VA": [
        "1000 Bank St, Richmond, VA 23219",
        "100 St Paul's Blvd, Norfolk, VA 23510",
        "2100 Clarendon Blvd, Arlington, VA 22201",
        "313 Park Ave, Falls Church, VA 22046",
        "401 Courthouse Sq, Alexandria, VA 22314",
        "600 E Main St, Richmond, VA 23219",
        "1 Waterside Dr, Norfolk, VA 23510",
        "300 Court Sq, Charlottesville, VA 22902",
        "10000 Main St, Fairfax, VA 22031",
        "150 Shenandoah Ave, Roanoke, VA 24016",
    ],
    "WA": [
        "416 Sid Snyder Ave SW, Olympia, WA 98504",
        "600 4th Ave, Seattle, WA 98104",
        "1301 Yakima St, Tacoma, WA 98405",
        "625 E Main St, Spokane, WA 99202",
        "1220 Main St, Vancouver, WA 98660",
        "200 Occidental Ave S, Seattle, WA 98104",
        "500 E 4th Ave, Olympia, WA 98501",
        "100 W Harrison St, Seattle, WA 98119",
        "800 Pike St, Seattle, WA 98101",
        "500 Wall St, Seattle, WA 98121",
    ],
    "WV": [
        "1900 Kanawha Blvd E, Charleston, WV 25305",
        "501 Avery St, Parkersburg, WV 26101",
        "1 Players Club Dr, Charles Town, WV 25414",
        "800 Quarrier St, Charleston, WV 25301",
        "200 Civic Center Dr, Charleston, WV 25301",
        "1000 Technology Dr, Fairmont, WV 26554",
        "50 Eleventh St, Wheeling, WV 26003",
        "100 Capitol St, Charleston, WV 25301",
        "300 Foxcroft Ave, Martinsburg, WV 25401",
        "1 Court Sq, Lewisburg, WV 24901",
    ],
    "WI": [
        "2 E Main St, Madison, WI 53703",
        "200 E Wells St, Milwaukee, WI 53202",
        "100 N Appleton St, Appleton, WI 54911",
        "625 E Wisconsin Ave, Milwaukee, WI 53202",
        "210 Martin Luther King Jr Blvd, Madison, WI 53703",
        "330 E Kilbourn Ave, Milwaukee, WI 53202",
        "1 S Pinckney St, Madison, WI 53703",
        "100 E Grand Ave, Eau Claire, WI 54701",
        "200 Main Ave, De Pere, WI 54115",
        "700 N Water St, Milwaukee, WI 53202",
    ],
    "WY": [
        "200 W 24th St, Cheyenne, WY 82001",
        "150 N Durbin St, Casper, WY 82601",
        "510 S Center St, Casper, WY 82601",
        "100 E 2nd St, Cheyenne, WY 82001",
        "1002 Sheridan Ave, Cody, WY 82414",
        "150 E Broadway Ave, Jackson, WY 83001",
        "200 E A St, Casper, WY 82601",
        "120 W 15th St, Cheyenne, WY 82002",
        "700 S Wolcott St, Casper, WY 82601",
        "200 Grand Ave, Laramie, WY 82070",
    ],
}

US_STATES = [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "DC", "FL",
    "GA", "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME",
    "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH",
    "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI",
    "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI",
    "WY",
]


def run_lookup(client: httpx.Client, address: str) -> dict:
    """Call the lookup API and return parsed result."""
    try:
        resp = client.get("/api/lookup", params={"address": address}, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        return {"error": str(e), "geocoded": None, "utilities": []}


def run_test(
    base_url: str,
    addresses: dict[str, list[str]],
    regions: list[str],
    csv_path: Path,
    region_label: str = "Region",
):
    fieldnames = [
        "region", "address", "geocoded", "geocoded_address",
        "lat", "lon", "utilities_found", "utility_names",
        "match_methods", "has_tariffs", "error",
    ]

    region_stats: dict[str, dict] = {
        r: {"tested": 0, "geocoded": 0, "with_utilities": 0, "with_tariffs": 0}
        for r in regions
    }

    all_rows: list[dict] = []
    total = sum(len(addresses[r]) for r in regions)
    done = 0

    with httpx.Client(base_url=base_url) as client:
        for region in regions:
            addrs = addresses[region]
            print(f"\n{'='*60}")
            print(f"  {region} — testing {len(addrs)} addresses")
            print(f"{'='*60}")

            for addr in addrs:
                done += 1
                data = run_lookup(client, addr)
                error = data.get("error", "")

                geo = data.get("geocoded")
                geocoded = "yes" if geo else "no"
                geocoded_addr = geo.get("formatted_address", "") if geo else ""
                lat = f"{geo['latitude']:.5f}" if geo else ""
                lon = f"{geo['longitude']:.5f}" if geo else ""

                utils = data.get("utilities", [])
                utilities_found = len(utils)
                utility_names = "; ".join(u["name"] for u in utils)
                match_methods = "; ".join(
                    sorted(set(u["match_method"] for u in utils))
                )
                has_tariffs = "yes" if any(
                    u.get("residential_tariff_count", 0) + u.get("commercial_tariff_count", 0) > 0
                    for u in utils
                ) else "no"

                row = {
                    "region": region,
                    "address": addr,
                    "geocoded": geocoded,
                    "geocoded_address": geocoded_addr,
                    "lat": lat,
                    "lon": lon,
                    "utilities_found": utilities_found,
                    "utility_names": utility_names,
                    "match_methods": match_methods,
                    "has_tariffs": has_tariffs,
                    "error": error,
                }
                all_rows.append(row)

                stats = region_stats[region]
                stats["tested"] += 1
                if geo:
                    stats["geocoded"] += 1
                if utilities_found > 0:
                    stats["with_utilities"] += 1
                if has_tariffs == "yes":
                    stats["with_tariffs"] += 1

                status = "OK" if utilities_found > 0 else ("GEO_FAIL" if not geo else "NO_MATCH")
                short_addr = addr[:50] + "..." if len(addr) > 50 else addr
                print(f"  [{done}/{total}] {status:>9}  {short_addr}")
                if utilities_found > 0:
                    for u in utils[:3]:
                        tariffs = u.get("residential_tariff_count", 0) + u.get("commercial_tariff_count", 0)
                        print(f"             -> {u['name']} ({u['match_method']}, {tariffs} tariffs)")
                    if len(utils) > 3:
                        print(f"             ... and {len(utils) - 3} more")

                time.sleep(1.5)

    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_rows)

    print(f"\n\n{'='*80}")
    print(f"  RESULTS SUMMARY")
    print(f"{'='*80}")
    print(f"{region_label:<10} {'Tested':>8} {'Geocoded':>10} {'With Utils':>12} {'With Tariffs':>14}")
    print(f"{'-'*10} {'-'*8} {'-'*10} {'-'*12} {'-'*14}")

    totals = {"tested": 0, "geocoded": 0, "with_utilities": 0, "with_tariffs": 0}
    for region in regions:
        s = region_stats[region]
        print(f"{region:<10} {s['tested']:>8} {s['geocoded']:>10} {s['with_utilities']:>12} {s['with_tariffs']:>14}")
        for k in totals:
            totals[k] += s[k]

    print(f"{'-'*10} {'-'*8} {'-'*10} {'-'*12} {'-'*14}")
    print(f"{'TOTAL':<10} {totals['tested']:>8} {totals['geocoded']:>10} {totals['with_utilities']:>12} {totals['with_tariffs']:>14}")
    print(f"\nCSV saved to: {csv_path}")

    return totals


def main():
    parser = argparse.ArgumentParser(description="Test address lookups")
    parser.add_argument("--base-url", default="http://localhost:8000")
    parser.add_argument("--country", choices=["US", "CA", "both"], default="US")
    args = parser.parse_args()

    results_dir = Path(__file__).parent.parent / "test_results"
    results_dir.mkdir(exist_ok=True)

    if args.country in ("CA", "both"):
        print("\n" + "=" * 80)
        print("  RUNNING CANADA ADDRESS TESTS")
        print("=" * 80)
        run_test(
            args.base_url,
            CA_ADDRESSES,
            CA_REGIONS,
            results_dir / "canada_lookup_results.csv",
            region_label="Province",
        )

    if args.country in ("US", "both"):
        print("\n" + "=" * 80)
        print("  RUNNING US ADDRESS TESTS")
        print("=" * 80)
        run_test(
            args.base_url,
            US_ADDRESSES,
            US_STATES,
            results_dir / "us_lookup_results.csv",
            region_label="State",
        )


if __name__ == "__main__":
    main()
