from ddgs import DDGS
from ddgs.exceptions import DDGSException
from pathlib import Path
from PIL import Image
from io import BytesIO
from multiprocessing.pool import ThreadPool
from typing import Dict, List, Tuple
from tqdm import tqdm
import requests

NUM_PROCESSES = 8
MAX_RESULTS = 300

ANIMALS_LIST = [
    "cat",
    "dog",
    "bear",
    "wolf",
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
    "lobster",
    "shrimp",
    "sea turtle",
    "sea horse",
]

PLUSH_LIST = [
    "stuffed animal",
    "plushie",
    "plush toy",
    "stuffed toy",
    "plush",
]

def make_queries() -> Dict[str, str]:
    """
    Creates queries for each animal and plush combination
    
    Returns
    -------
        Dict[str, str]
            A dictionary of queries
    """
    print("Making queries")
    queries = dict()
    for animal in ANIMALS_LIST:
        queries[animal] = list()
        for plush in PLUSH_LIST:
            # We want to query for both {animal} {plush} and {plush} {animal}
            # because the order of the words in the query can affect the results
            queries[animal].append(f"{animal} {plush}")
            queries[animal].append(f"{plush} {animal}")
    return queries

def download_images(images: List[Dict[str, str]], folder: Path):
    """
    Downloads images to a folder

    Arguments
    ----------
        images: List[Dict[str, str]]
            A list of image dictionaries
            - title: The image title
            - image: The image URL
            - thumbnail: The image thumbnail URL
            - url: The website URL
            - height: The image height
            - width: The image width
            - source: The image source
        folder: Path
            The folder to save the images to
    """
    for image in images:
        try:
            url = image["image"]
            filename = url.split("/")[-1]
            path = folder / filename

            r = requests.get(url)
            r.raise_for_status()
            
            pil_image = Image.open(BytesIO(r.content))
            pil_image.save(path, format="JPEG", quality=95)
            pil_image.close()
        except requests.exceptions.RequestException as e:
            print(f"Error downloading {image['image']}: {e}")
        except Exception as e:
            print(f"Error downloading {image['image']}: {e}")

def _download_worker(animal_images: Tuple[str, List[Dict[str, str]]]):
    """Worker for pool: downloads images for one animal."""
    animal, images = animal_images
    folder = Path("images") / animal
    folder.mkdir(parents=True, exist_ok=True)
    download_images(images, folder)
    return {"animal": animal, "length": len(images)}

def search_images(query: str, max_results: int = 100) -> List[Dict[str, str]]:
    """
    Searches for images on the web using the DDGS library
    
    Arguments
    ----------
        query: str
            The query to search for in the format "{animal} {plush}" or "{plush} {animal}"
        max_results: int
            The maximum number of results to return
    
    Returns
    -------
        List[Dict[str, str]]
            A list of image dictionaries
            - title: The image title
            - image: The image URL
            - thumbnail: The image thumbnail URL
            - url: The URL of the website that the image is from
            - height: The image height
            - width: The image width
            - source: The image source
    """
    try:
        with DDGS() as ddgs:
            images = ddgs.images(
                query=query,
                safesearch="off",
                color="color",
                type_image="photo",
                max_results=max_results,
            )
            print(f"Found {len(images)} images for {query}")
            return list(images)
    except DDGSException as e:
        print(f"No results for '{query}': {e}")
        return []

def get_images():
    """
    Creates queries for each animal and plush combination and downloads the images 
    """
    print("Starting to get images")
    queries: Dict[str, List[str]] = make_queries()
    images_by_animal: Dict[str, List[Dict[str, str]]] = dict()

    # Search for images for each animal and plush combination
    for animal, queries in queries.items():
        images_by_animal[animal] = []
        num_results = MAX_RESULTS // len(queries) # Each query gets an equal number of results
        for query in queries:
            images = search_images(query, max_results=num_results)
            images_by_animal[animal].extend(images)

    # Download each animal's images in parallel
    with ThreadPool(processes=NUM_PROCESSES) as p:
        results = p.imap_unordered(_download_worker, images_by_animal.items())
        for result in results:
            print(f"{result['animal']}: {result['length']} images downloaded")

if __name__ == "__main__":
    get_images()