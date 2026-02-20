import argparse
import json
import multiprocessing
import os
from pathlib import Path

from typing import Dict, List, Tuple

from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

DIR_PATH = Path(__file__).parent
PROJECT_ROOT = DIR_PATH.parent

ANIMALS_LIST = [
    "cat",
    "dog",
    "bear",
    "fox",
    "rabbit",
    "snake",
    "tiger",
    "lion",
    "elephant",
    "panda",
    "koala",
    "whale",
    "dolphin",
    "shark",
    "octopus",
    "squid",
    "crab",
    "shrimp",
    "sea turtle",
]

PLUSH_LIST = [
    "stuffed animal",
    "plushie",
    "plush toy",
    "stuffed toy",
    "plush",
]

def make_queries() -> Dict[str, List[str]]:
    """
    Creates queries for each animal and plush combination

    Returns
    -------
        Dict[str, List[str]]
            A dictionary mapping animal name to list of query strings.
    """
    queries = dict()
    for animal in ANIMALS_LIST:
        queries[animal] = list()
        for plush in PLUSH_LIST:
            # We want to query for both {animal} {plush} and {plush} {animal}
            # because the order of the words in the query can affect the results
            queries[animal].append(f"{animal} {plush}")
            queries[animal].append(f"{plush} {animal}")
    return queries

def _run_crawl_worker(
    worker_index: int, 
    pairs: List[Tuple[str, str]],
    images_store: str,
    scraper_dir: str,
) -> None:
    """
    Run one Scrapy CrawlerProcess for the given (animal, query) pairs.

    Arguments
    ----------
    worker_index : int
        The index of the worker.
    pairs : List[Tuple[str, str]]
        The list of (animal, query) pairs.
    images_store : str
        The directory to store the images.
    scraper_dir : str
        The directory to store the scraper.
    """
    if not pairs:
        return
    animals = sorted({p[0] for p in pairs})
    print(f"Worker {worker_index}: crawling {len(animals)} animals: {animals}")
    os.chdir(scraper_dir)
    settings = get_project_settings()
    settings.set("IMAGES_STORE", images_store)
    process = CrawlerProcess(settings)
    process.crawl("bing_images", queries_json=json.dumps(pairs))
    process.start()


def _partition_list(items: List[str], n: int) -> List[List[str]]:
    """
    Split items into n equally sized chunks.

    Arguments
    ----------
    items : List[str]
        The list of items to partition.
    n : int
        The number of chunks to create.

    Returns
    -------
    List[List[str]]
        A list of lists, each containing items / n items.
    """
    if n <= 0:
        return [items] if items else []
    if n >= len(items):
        return [[x] for x in items]
    chunks = [[] for _ in range(n)]
    for i, x in enumerate(items):
        chunks[i % n].append(x)
    return chunks

def setup_args() -> argparse.Namespace:
    """
    Set up command line arguments.

    Returns
    -------
    argparse.Namespace
        The parsed arguments.
    """
    parser = argparse.ArgumentParser(description="Run Bing image scraper for plush animals.")
    parser.add_argument(
        "--workers",
        type=int,
        default=None,
        help="Number of parallel crawler processes (default: 4 or BING_SCRAPER_WORKERS env)",
    )
    return parser.parse_args()

def main() -> None:
    args = setup_args()

    n_workers = args.workers
    if n_workers is None:
        n_workers = int(os.environ.get("BING_SCRAPER_WORKERS", "4"))
    n_workers = max(1, n_workers)

    queries_by_animal = make_queries()
    animals = list(queries_by_animal.keys())
    if not animals:
        print("No animals from make_queries().")
        return

    # Partition animals into n_workers chunk
    n_workers = min(n_workers, len(animals)) # cap workers at number of animals
    animal_chunks = _partition_list(animals, n_workers)

    # Build (animal, query) pairs per chunk and run crawls in a process pool
    scraper_dir = str(DIR_PATH / "image_scraper")
    images_store = str(PROJECT_ROOT / "images")

    tasks = []
    for i, chunk in enumerate(animal_chunks):
        if not chunk:
            continue
        pairs = [(animal, query) for animal in chunk for query in queries_by_animal[animal]]
        tasks.append((i, pairs, images_store, scraper_dir))

    with multiprocessing.Pool(n_workers) as pool:
        pool.starmap(_run_crawl_worker, tasks)

    print("All workers finished.")


if __name__ == "__main__":
    main()
