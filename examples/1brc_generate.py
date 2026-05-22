#!/usr/bin/env python3
"""Generate the 1BRC dataset: 1 billion rows of station;temperature"""
import sys
import random
import os

STATIONS = [
    "Abha", "Abidjan", "Abéché", "Accra", "Addis Ababa", "Adelaide", "Aden",
    "Ahvaz", "Albuquerque", "Alexandra", "Alexandria", "Algiers", "Alice Springs",
    "Almaty", "Amsterdam", "Anadyr", "Anchorage", "Andorra la Vella", "Ankara",
    "Antananarivo", "Antsiranana", "Arkhangelsk", "Ashgabat", "Asmara",
    "Assab", "Astana", "Athens", "Atlanta", "Auckland", "Austin", "Baghdad",
    "Baguio", "Baku", "Bangkok", "Bangui", "Banjul", "Barcelona", "Bata",
    "Batumi", "Beijing", "Beirut", "Belgrade", "Belmopan", "Berlin", "Bilbao",
    "Bissau", "Blantyre", "Bloemfontein", "Boise", "Bordeaux", "Bosaso",
    "Boston", "Bouaké", "Bratislava", "Brazzaville", "Bridgetown", "Brisbane",
    "Brussels", "Bucharest", "Budapest", "Buenos Aires", "Bujumbura", "Bulawayo",
    "Bur Sudan", "Busan", "Cabo San Lucas", "Cairo", "Calgary", "Canberra",
    "Cape Town", "Caracas", "Casablanca", "Cayenne", "Chicago", "Chisinau",
    "Chita", "Chongqing", "Colombo", "Conakry", "Copenhagen", "Cotonou",
    "Dakar", "Dallas", "Dammam", "Dar es Salaam", "Darwin", "Davao",
    "Delhi", "Denver", "Detroit", "Dili", "Djibouti", "Dodoma", "Doha",
    "Dubai", "Dubbo", "Dublin", "Dunedin", "Dushanbe", "Edinburgh",
    "Edmonton", "El Paso", "Entebbe", "Erbil", "Erevan", "Fairbanks",
    "Fianarantsoa", "Freetown", "Fukuoka", "Gaborone", "Geneva", "Genoa",
    "Georgetown", "Gibraltar", "Guatemala City", "Guayaquil", "Hamburg",
    "Hanoi", "Harare", "Harbin", "Helsinki", "Ho Chi Minh City", "Hong Kong",
    "Honiara", "Honolulu", "Houston", "Hyderabad", "Ifrane", "Istanbul",
    "Jacksonville", "Jakarta", "Jerusalem", "Johannesburg", "Kabul", "Kampala",
    "Karachi", "Kathmandu", "Khartoum", "Kiev", "Kigali", "Kinshasa",
    "Kolkata", "Kuala Lumpur", "Kuwait City", "Lagos", "Las Vegas", "Libreville",
    "Lima", "Lisbon", "Ljubljana", "Lomé", "London", "Los Angeles", "Luanda",
    "Lusaka", "Lyon", "Madrid", "Malé", "Managua", "Manila", "Maputo",
    "Maracaibo", "Marrakech", "Maseru", "Mbabane", "Mexico City", "Miami",
    "Milan", "Minneapolis", "Minsk", "Mogadishu", "Monrovia", "Monterrey",
    "Montevideo", "Montreal", "Moroni", "Moscow", "Mosul", "Mumbai",
    "Munich", "Muscat", "N'Djamena", "Nairobi", "Naples", "Nashville",
    "Nassau", "Niamey", "Nicosia", "Nouakchott", "Nouméa", "Novosibirsk",
    "Nuku'alofa", "Odesa", "Omaha", "Osaka", "Oslo", "Ottawa", "Ouagadougou",
    "Palembang", "Panama City", "Paramaribo", "Paris", "Perth", "Philadelphia",
    "Phnom Penh", "Phoenix", "Port Louis", "Port Moresby", "Port Sudan",
    "Porto", "Porto-Novo", "Prague", "Pyongyang", "Québec City", "Quito",
    "Rabat", "Reykjavik", "Riga", "Riyadh", "Rome", "Roseau",
    "Saint-Denis", "Salt Lake City", "San Francisco", "San José", "San Juan",
    "Santiago", "Santo Domingo", "São Paulo", "Seattle", "Seoul", "Shanghai",
    "Singapore", "Skopje", "Sofia", "Stockholm", "Suva", "Sydney",
    "Taipei", "Tallinn", "Tashkent", "Tbilisi", "Tegucigalpa", "Tehran",
    "Tokyo", "Toronto", "Tripoli", "Tunis", "Ulaanbaatar", "Vancouver",
    "Vienna", "Vientiane", "Vilnius", "Warsaw", "Washington, D.C.",
    "Wellington", "Windhoek", "Winnipeg", "Yamoussoukro", "Yangon",
    "Yaounde", "Yekaterinburg", "Yerevan", "Zagreb", "Zurich",
]

STATION_TEMPS = {}
rng = random.Random(42)
for s in STATIONS:
    mean = rng.uniform(-30, 40)
    STATION_TEMPS[s] = mean

def main():
    rows = int(sys.argv[1]) if len(sys.argv) > 1 else 1_000_000_000
    out  = sys.argv[2] if len(sys.argv) > 2 else "measurements.csv"

    print(f"Generating {rows:,} rows → {out}", flush=True)

    station_list = list(STATION_TEMPS.keys())
    means        = [STATION_TEMPS[s] for s in station_list]
    n            = len(station_list)

    CHUNK = 500_000
    buf   = []

    with open(out, "w", buffering=8*1024*1024) as f:
        f.write("station;temperature\n")
        for i in range(rows):
            idx  = rng.randrange(n)
            temp = round(means[idx] + rng.uniform(-10, 10), 1)
            buf.append(f"{station_list[idx]};{temp}\n")
            if len(buf) == CHUNK:
                f.writelines(buf)
                buf.clear()
                if (i + 1) % 50_000_000 == 0:
                    pct = (i + 1) * 100 // rows
                    print(f"  {pct}% — {(i+1)//1_000_000}M rows written", flush=True)
        if buf:
            f.writelines(buf)

    size = os.path.getsize(out)
    print(f"Done. File size: {size / 1e9:.2f} GB", flush=True)

if __name__ == "__main__":
    main()
