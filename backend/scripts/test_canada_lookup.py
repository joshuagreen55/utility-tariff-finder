"""
Canada Address Lookup Test Script

Tests the address lookup API with 10 real addresses per Canadian province/territory
(130 total) and outputs results to CSV and a summary table.

Usage:
    python -m scripts.test_canada_lookup [--base-url http://localhost:8000]
"""

import argparse
import csv
import os
import time
from pathlib import Path

import httpx

ADDRESSES: dict[str, list[str]] = {
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

PROVINCES = [
    "AB", "BC", "MB", "NB", "NL", "NS", "NT", "NU",
    "ON", "PE", "QC", "SK", "YT",
]


def run_lookup(client: httpx.Client, address: str) -> dict:
    """Call the lookup API and return parsed result."""
    try:
        resp = client.get("/api/lookup", params={"address": address}, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        return {"error": str(e), "geocoded": None, "utilities": []}


def main():
    parser = argparse.ArgumentParser(description="Test Canadian address lookups")
    parser.add_argument("--base-url", default="http://localhost:8000")
    args = parser.parse_args()

    results_dir = Path(__file__).parent.parent / "test_results"
    results_dir.mkdir(exist_ok=True)
    csv_path = results_dir / "canada_lookup_results.csv"

    fieldnames = [
        "province", "address", "geocoded", "geocoded_address",
        "lat", "lon", "utilities_found", "utility_names",
        "match_methods", "has_tariffs", "error",
    ]

    province_stats: dict[str, dict] = {
        p: {"tested": 0, "geocoded": 0, "with_utilities": 0, "with_tariffs": 0}
        for p in PROVINCES
    }

    all_rows: list[dict] = []
    total = sum(len(addrs) for addrs in ADDRESSES.values())
    done = 0

    with httpx.Client(base_url=args.base_url) as client:
        for prov in PROVINCES:
            addrs = ADDRESSES[prov]
            print(f"\n{'='*60}")
            print(f"  {prov} — testing {len(addrs)} addresses")
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
                    "province": prov,
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

                stats = province_stats[prov]
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

                time.sleep(2.0)

    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_rows)

    print(f"\n\n{'='*80}")
    print(f"  RESULTS SUMMARY")
    print(f"{'='*80}")
    print(f"{'Province':<10} {'Tested':>8} {'Geocoded':>10} {'With Utils':>12} {'With Tariffs':>14}")
    print(f"{'-'*10} {'-'*8} {'-'*10} {'-'*12} {'-'*14}")

    totals = {"tested": 0, "geocoded": 0, "with_utilities": 0, "with_tariffs": 0}
    for prov in PROVINCES:
        s = province_stats[prov]
        print(f"{prov:<10} {s['tested']:>8} {s['geocoded']:>10} {s['with_utilities']:>12} {s['with_tariffs']:>14}")
        for k in totals:
            totals[k] += s[k]

    print(f"{'-'*10} {'-'*8} {'-'*10} {'-'*12} {'-'*14}")
    print(f"{'TOTAL':<10} {totals['tested']:>8} {totals['geocoded']:>10} {totals['with_utilities']:>12} {totals['with_tariffs']:>14}")
    print(f"\nCSV saved to: {csv_path}")


if __name__ == "__main__":
    main()
