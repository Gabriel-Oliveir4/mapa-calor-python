import re, time, sqlite3, hashlib
from datetime import datetime, timezone
from typing import List, Dict, Tuple, Optional
import requests, feedparser
from readability import Document
from bs4 import BeautifulSoup
import spacy
from langdetect import detect
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from datasketch import MinHash, MinHashLSH
import folium
from folium.plugins import HeatMap

CRIME_KEYWORDS = {
    "pt": ["homicídio","assassinato","roubo","furto","tráfico","agressão","sequestro","extorsão",
           "estupro","latrocínio","corrupção","fraude","crime organizado","milícia","tiroteio"],
    "en": ["homicide","murder","robbery","theft","trafficking","assault","kidnapping","extortion",
           "rape","felony","corruption","fraud","organized crime","shooting","arson"]
}

# retorna timestamp UTC ISO8601
def dt_utc() -> str:
    return datetime.now(timezone.utc).isoformat()

# lê RSS e filtra itens por data mínima
def fetch_rss_items(urls: List[str], since_iso: str) -> List[Dict]:
    out = []
    since = datetime.fromisoformat(since_iso.replace("Z","+00:00"))
    for u in urls:
        f = feedparser.parse(u)
        for e in f.entries:
            link = e.get("link"); title = (e.get("title") or "").strip()
            pub = e.get("published_parsed") or e.get("updated_parsed")
            if not link or not title: 
                continue
            if pub:
                pub_dt = datetime(*pub[:6], tzinfo=timezone.utc)
                if pub_dt < since: 
                    continue
                pub_iso = pub_dt.isoformat()
            else:
                pub_iso = dt_utc()
            out.append({"title": title, "link": link, "published_at": pub_iso})
    return out

# baixa HTML e extrai texto legível
def fetch_clean_text(url: str, timeout=20) -> str:
    r = requests.get(url, timeout=timeout, headers={"User-Agent":"Mozilla/5.0 MLNews/2025"})
    r.raise_for_status()
    html = r.text
    doc = Document(html)
    cleaned_html = doc.summary()
    text = BeautifulSoup(cleaned_html, "lxml").get_text(" ", strip=True)
    return re.sub(r"\s+", " ", text)

# carrega pipelines spaCy (en/pt) com fallback
def load_pipelines() -> Dict[str, spacy.Language]:
    nlp = {}
    try: 
        nlp["en"] = spacy.load("en_core_web_lg")
    except: 
        nlp["en"] = spacy.load("xx_ent_wiki_sm")
    try: 
        nlp["pt"] = spacy.load("pt_core_news_lg")
    except: 
        nlp["pt"] = spacy.load("xx_ent_wiki_sm")
    return nlp

# detecta idioma do texto
def lang_of(text: str) -> str:
    try:
        l = detect(text[:2000])
        return "pt" if l.startswith("pt") else "en"
    except: 
        return "en"

# calcula score de “crime” por keywords e tamanho
def crime_score(text: str, lang: str) -> float:
    t = text.lower()
    hits = sum(1 for k in CRIME_KEYWORDS[lang] if k in t)
    length_norm = min(len(text)/2000.0, 1.0)
    return min(hits/5.0, 1.0)*0.6 + length_norm*0.4

# extrai entidades de localização (GPE/LOC)
def extract_places(text: str, nlp: Dict[str, spacy.Language], lang: str) -> List[str]:
    doc = nlp[lang](text)
    vals = [ent.text for ent in doc.ents if ent.label_ in {"GPE","LOC"}]
    uniq, seen = [], set()
    for v in vals:
        k = v.strip()
        if k and k.lower() not in seen:
            seen.add(k.lower()); uniq.append(k)
    return uniq[:5]

# geocodifica nomes para lat/lon
def geocode_places(names: List[str]) -> List[Tuple[float,float,str]]:
    geoloc = Nominatim(user_agent="mlnews-geo")
    rate = RateLimiter(geoloc.geocode, min_delay_seconds=1, swallow_exceptions=True)
    out = []
    for n in names:
        g = rate(n)
        if g and g.latitude and g.longitude:
            out.append((g.latitude, g.longitude, n))
    return out

# gera assinatura MinHash do texto
def text_signature(text: str, num_perm=128) -> MinHash:
    m = MinHash(num_perm=num_perm)
    tokens = set(re.findall(r"\w{5,}", text.lower()))
    for t in tokens: 
        m.update(t.encode())
    return m

# cria índice LSH para deduplicação
def build_lsh(threshold=0.85, num_perm=128):
    return MinHashLSH(threshold=threshold, num_perm=num_perm)

# garante schema SQLite
def ensure_db(db="events.db"):
    con = sqlite3.connect(db); cur = con.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS news_events(
        id INTEGER PRIMARY KEY,
        link TEXT UNIQUE,
        title TEXT,
        published_at TEXT,
        lang TEXT,
        score REAL,
        lat REAL,
        lon REAL,
        place TEXT,
        created_at TEXT
    )""")
    con.commit(); con.close()

# insere eventos no banco
def save_events(rows: List[Tuple], db="events.db"):
    con = sqlite3.connect(db); cur = con.cursor()
    cur.executemany("""INSERT OR IGNORE INTO news_events
        (link,title,published_at,lang,score,lat,lon,place,created_at)
        VALUES (?,?,?,?,?,?,?,?,?)""", rows)
    con.commit(); con.close()

# agrega pontos para o heatmap
def aggregate(db="events.db") -> List[Tuple[float,float,int]]:
    con = sqlite3.connect(db); cur = con.cursor()
    cur.execute("""SELECT round(lat,3), round(lon,3), COUNT(*)
                   FROM news_events GROUP BY round(lat,3), round(lon,3)""")
    rows = cur.fetchall(); con.close()
    return rows

# gera e salva o heatmap
def build_heatmap(points: List[Tuple[float,float,int]], outfile="heatmap.html"):
    m = folium.Map(location=[-15.78,-47.93], zoom_start=3)
    data = [[lat, lon, count] for lat, lon, count in points]
    HeatMap(data, radius=18, blur=14, max_zoom=6).add_to(m)
    m.save(outfile)
    return outfile

# pipeline principal: coleta, filtra, deduplica, geocodifica e mapeia
def run_pipeline(rss_urls: List[str], since="2024-01-01T00:00:00+00:00", top_n=300):
    ensure_db()
    nlp = load_pipelines()
    items = fetch_rss_items(rss_urls, since)[:top_n]
    lsh = build_lsh(); sig_index = {}

    to_save = []
    for it in items:
        try:
            text = fetch_clean_text(it["link"])
            lang = lang_of(text)
            s = crime_score(text, lang)
            if s < 0.6: 
                continue
            sig = text_signature(text)
            if lsh.query(sig):
                continue
            key = hashlib.sha1(it["link"].encode()).hexdigest()
            lsh.insert(key, sig); sig_index[key] = sig

            places = extract_places(text, nlp, lang)
            geos = geocode_places(places)
            if not geos: 
                continue
            lat, lon, place = geos[0]
            to_save.append((
                it["link"], it["title"], it["published_at"], lang, float(s),
                float(lat), float(lon), place, dt_utc()
            ))
            time.sleep(0.5)
        except Exception:
            continue

    if to_save:
        save_events(to_save)
    pts = aggregate()
    out = build_heatmap(pts)
    return {"inserted": len(to_save), "points": len(pts), "heatmap_file": out}

if __name__ == "__main__":
    RSS = [
        "https://g1.globo.com/rss/g1/",
        "https://rss.nytimes.com/services/xml/rss/nyt/World.xml",
        "https://feeds.bbci.co.uk/news/world/rss.xml",
    ]
    res = run_pipeline(RSS, since="2024-01-01T00:00:00+00:00", top_n=300)
    print(res)
