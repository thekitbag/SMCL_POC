# processing.py
import requests
import pandas as pd
import os
import logging

from dotenv import load_dotenv

from data_cleaning import clean_data


load_dotenv()

zendesk_subdomain = os.environ.get("ZENDESK_SUBDOMAIN")
zendesk_email = os.environ.get("ZENDESK_EMAIL")
zendesk_token = os.environ.get("ZENDESK_API_TOKEN")

zendesk_api_base_url = f"https://{zendesk_subdomain}.zendesk.com/api/v2"

# Authentication
auth = (f"{zendesk_email}/token", zendesk_token)



def process_webhook_data(data, testing=False):
    """
    Processes the webhook data, cleans it, creates or updates users, and updates the ticket.

    Args:
        data (dict or pd.DataFrame): The webhook data (dict) or test data (DataFrame).
        testing (bool): True if running locally, False if on PythonAnywhere.
    """
    try:
        if testing:
            # Local testing: Process the DataFrame
            df_cleaned = clean_data(data)

            # Simulate user creation for testing (don't call Zendesk API)
            print("Simulating user creation for testing...")
            for index, row in df_cleaned.iterrows():
                print(f"  Creating user: {row['EmailAddress']}")
            return True  # Indicate success

        else:
            # PythonAnywhere: Process the webhook data
            ticket_id = data.get('ticket_id')

            # Fetch attachments from Zendesk ticket (no Zenpy)
            attachments = get_attachments_from_ticket(ticket_id)

            if not attachments:
                update_zendesk_ticket(ticket_id, None, "No attachments found in this ticket")
                return False

            attachment = attachments[0]  # Assume first attachment is the CSV

            try:
                response = requests.get(attachment['url'], auth=auth, stream=True)
                response.raise_for_status()  # Raise an exception for bad status codes
                df = pd.read_csv(response.raw)
            except Exception as e:
                update_zendesk_ticket(ticket_id, None, "Could not read attachment as CSV")
                return False

            df_cleaned = clean_data(df)

            # Create or update users in Zendesk
            user_create_or_update_results = create_or_update_users(df_cleaned)

            # Update the Zendesk ticket
            update_zendesk_ticket(ticket_id, user_create_or_update_results)
            return True  # Indicate success

    except Exception as e:
        logging.error(f"Error in process_webhook_data: {e}", exc_info=True)
        return False  # Indicate failure

def create_or_update_users(df_cleaned):
    """
    Creates or updates users in Zendesk via the API.

    Args:
        df_cleaned (pd.DataFrame): The cleaned DataFrame with user data.

    Returns:
        dict: Results of the create or update operation.
    """
    url = f"{zendesk_api_base_url}/users/create_or_update_many.json"
    headers = {"Content-Type": "application/json"}

    user_list = []
    for index, row in df_cleaned.iterrows():
        user_data = {
            "user": {  # Note: "user" is nested in the JSON payload
                "name": row['DisplayName'],
                "email": row['EmailAddress'],
                # Add other fields as needed
                "verified": True,
                "remote_photo_url": "",
                "custom_fields": []
            }
        }
        user_list.append(user_data)

    # Zendesk's API expects an array of users under a "users" key
    payload = {"users": user_list}

    try:
        response = requests.post(url, headers=headers, auth=auth, json=payload)
        response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
        return response.json()  # Assuming the API returns JSON
    except requests.exceptions.RequestException as e:
        logging.error(f"Error creating/updating users: {e}", exc_info=True)
        return None

def get_attachments_from_ticket(ticket_id):
    """
    Fetches attachments from a Zendesk ticket using the Zendesk API.

    Args:
        ticket_id (int): The ID of the Zendesk ticket.

    Returns:
        list: A list of attachment objects (dictionaries) or None if an error occurs.
    """
    url = f"{zendesk_api_base_url}/tickets/{ticket_id}/attachments.json"
    headers = {"Content-Type": "application/json"}

    try:
        response = requests.get(url, auth=auth, headers=headers)
        response.raise_for_status()
        data = response.json()
        return data.get("attachments", [])  # Return the list of attachments
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching attachments for ticket {ticket_id}: {e}", exc_info=True)
        return None

def update_zendesk_ticket(ticket_id, job_status, error_details=None):
    """
    Updates the Zendesk ticket with the upload results using the Zendesk API.

    Args:
        ticket_id (int): The ID of the Zendesk ticket.
        job_status (dict or None): The status object from bulk user creation/update.
        error_details (str): Error details, if any.
    """
    url = f"{zendesk_api_base_url}/tickets/{ticket_id}.json"
    headers = {"Content-Type": "application/json"}

    try:
        if job_status is None:
            comment_body = error_details if error_details else "Error processing user update."
            ticket_status = 'open'
        elif job_status and job_status['status'] == "completed": # Check for job completion
            comment_body = f"User upload processed.\nDetails:\n {job_status['details']}"
            ticket_status = 'solved'
        else:
            comment_body = f"User upload failed.\nDetails:\n {job_status['details'] if job_status else error_details}"
            ticket_status = 'open'

        payload = {
            "ticket": {
                "status": ticket_status,
                "comment": {
                    "body": comment_body,
                    "public": False
                }
            }
        }

        response = requests.put(url, headers=headers, auth=auth, json=payload)
        response.raise_for_status()

    except requests.exceptions.RequestException as e:
        logging.error(f"Error updating ticket: {e}", exc_info=True)