from API_KEY import api_key
import requests

def extract():
    url = 'https://newsapi.org/v2/everything'

    params = {
        'q': 'bitcoin',
        'from': '2024-03-25',
        'sortBy': 'popularity',
        'language': 'en',
        'apiKey': api_key,
    }

    try:
        response = requests.get(url, params=params)
        # raise an http error if the responds status code is 400 (Bad Request) or higher
        response.raise_for_status()
        return response.json()['articles']
           
    except requests.exceptions.HTTPError as errh:
        error_msg = f"HTTPError: {errh}"
        if errh.response.status_code == 403:
            error_msg += " Access forbidden: Check if your API token has permissions or if you've hit a rate limit."
        print(error_msg)
    except requests.exceptions.ConnectionError:
        print("Error Connecting: Please check your network connection.")
    except requests.exceptions.Timeout as errt:
        print("Timeout Error: The request timed out. You might want to try again later.")
    except requests.exceptions.RequestException as err:
        print("An error occurred:", err)

    # Return None or a suitable default value in case of an error
    return None
 
extract()



