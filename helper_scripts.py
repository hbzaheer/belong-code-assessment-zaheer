




def is_valid_response(response):
    if response.status_code != 200:
        print(f"Error: {response.status_code} - {response.url}")
        return False
    return True