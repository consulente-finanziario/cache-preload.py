#!/usr/bin/env python3

import requests
import xml.etree.ElementTree as ET
import threading
import queue
import time

# Elenco delle sitemap da cui estrarre le URL
SITEMAPS = [
    "https://consulente-finanziario.org/author-sitemap.xml",
    "https://consulente-finanziario.org/page-sitemap.xml",
    "https://consulente-finanziario.org/post-sitemap.xml",
    "https://consulente-finanziario.org/post-sitemap2.xml",
]

# Numero massimo di thread (connessioni concorrenti)
THREAD_COUNT = 3

# Header per chiudere la connessione al termine di ogni richiesta
HEADERS = {
    'Connection': 'close'
}

# Lista thread-safe di URL non visitate correttamente al primo passaggio
failed_urls = []
lock = threading.Lock()

# Contatori globali per progresso e calcolo velocità
processed_count = 0
start_time = 0.0
total = 0

def fetch_sitemap_urls(sitemap_url):
    r = requests.get(sitemap_url, timeout=30, headers=HEADERS)
    r.raise_for_status()
    content = r.content  # forza il download completo
    r.close()
    tree = ET.fromstring(content)
    ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    return [loc.text for loc in tree.findall(".//sm:loc", namespaces=ns)]

def worker(url_queue):
    global processed_count
    while True:
        url = url_queue.get()
        if url is None:
            break

        r = None
        ok = False
        status = None

        # scarico la pagina e chiudo la connessione
        t0 = time.time()
        try:
            r = requests.get(url, timeout=30, headers=HEADERS)
            status = r.status_code
            ok = r.ok
        except Exception as e:
            status = f"Error: {e}"
            ok = False
        finally:
            if r is not None:
                # la risposta è già stata letta internamente da requests.get()
                r.close()

        # aggiorno contatore e calcolo velocità media
        with lock:
            processed_count += 1
            idx = processed_count
            elapsed = time.time() - start_time
            speed = idx / elapsed if elapsed > 0 else 0.0

        # stampo stato, progresso e velocità
        print(f"[{threading.current_thread().name}] "
              f"{idx}/{total} {url} -> {status} "
              f"({speed:.2f} req/s)")

        # se fallita la salvo per retry
        if not ok:
            with lock:
                failed_urls.append(url)

        url_queue.task_done()

if __name__ == '__main__':
    # 1) Estrazione URL dalle sitemap
    all_urls = []
    for sitemap in SITEMAPS:
        print(f"Parsing sitemap: {sitemap}")
        try:
            urls = fetch_sitemap_urls(sitemap)
            print(f"Trovate {len(urls)} pagine da {sitemap}")
            all_urls.extend(urls)
        except Exception as e:
            print(f"Errore nel parsing di {sitemap}: {e}")

    # 2) Unici e conteggio totale
    unique_urls = list(set(all_urls))
    total = len(unique_urls)
    print(f"Totale pagine uniche da visitare: {total}")

    # 3) Preparo coda
    url_queue = queue.Queue()
    for u in unique_urls:
        url_queue.put(u)

    # 4) Inizializzo timer e contatore
    start_time = time.time()
    processed_count = 0

    # 5) Avvio worker
    threads = []
    for i in range(THREAD_COUNT):
        t = threading.Thread(
            target=worker,
            args=(url_queue,),
            name=f"Worker-{i+1}",
            daemon=True
        )
        t.start()
        threads.append(t)

    # 6) Attendo fine coda
    url_queue.join()
    for _ in threads:
        url_queue.put(None)
    for t in threads:
        t.join()

    # 7) Retry falliti
    still_failed = []
    if failed_urls:
        unique_failed = list(set(failed_urls))
        print(f"\nRetrying {len(unique_failed)} pagine fallite una seconda volta...")
        for url in unique_failed:
            try:
                r = requests.get(url, timeout=30, headers=HEADERS)
                print(f"[RETRY] {url} -> {r.status_code}")
                if not r.ok:
                    still_failed.append(url)
                r.close()
            except Exception as e:
                print(f"[RETRY] Errore {url}: {e}")
                still_failed.append(url)
    else:
        print("\nNessuna pagina fallita nel primo passaggio.")

    # 8) Riepilogo finale
    success_count = total - len(still_failed)
    print(f"\nCaricate correttamente {success_count} pagine su {total} totali.")
    input("\nPremi Invio per terminare lo script...")
