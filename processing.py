# processing.py
import requests
import pandas as pd
import os
from zenpy import Zenpy
from data_cleaning import clean_data


zendesk_subdomain = os.environ.get("ZENDESK_SUBDOMAIN")
zendesk_email = os.environ.get("ZENDESK_EMAIL")
zendesk_token = os.environ.get("ZENDESK_API_TOKEN")

creds = {
    'email': zendesk_email,
    'token': zendesk_token,
    'subdomain': zendesk_subdomain
}

zenpy_client = Zenpy(**creds)

def process_webhook_data(data):
    """
    Processes the webhook data, cleans it, creates users, and updates the ticket.

    Args:
        data (dict): The JSON data from the webhook.
    """
    ticket_id = data.get('ticket_id')

    # Get attachments
    ticket = zenpy_client.tickets(id=ticket_id)
    attachments = ticket.attachments

    if not attachments:
        update = {
            'comment': {
                'body': f"No attachments found in this ticket",
                "public": False
            }
        }
        zenpy_client.tickets.update(ticket_id, **update)
        return

    # Download the first attachment (assuming only one file is uploaded)
    attachment = attachments[0]
    file_content = requests.get(attachment.url, stream=True).content

    # Read the file into a pandas DataFrame
    try:
        df = pd.read_excel(file_content)  # Or pd.read_csv(file_content, encoding='utf-8') if it's a CSV
    except Exception as e:
        update = {
            'comment': {
                'body': f"Could not read attachment as excel or csv",
                "public": False
            }
        }
        zenpy_client.tickets.update(ticket_id, **update)
        return

    # Clean the data
    df_cleaned = clean_data(df)

    # Create users in Zendesk
    user_data_list = df_cleaned.to_dict('records')
    job_status = zenpy_client.users.create_many(user_data_list)


    # Update the Zendesk ticket
    update_zendesk_ticket(ticket_id, job_status)


def update_zendesk_ticket(ticket_id, job_status, error_details=None):
    """
    Updates the Zendesk ticket with the upload results.

    Args:
        ticket_id (int): The ID of the Zendesk ticket.
        job_status (zenpy.lib.api_objects.JobStatus): The status object from bulk user creation
        error_details (str, optional): Details of any errors encountered. Defaults to None.
    """
    if job_status.status == "completed":

        update = {
            'status': 'solved',
            'comment': {
                'body': f"User upload processed.\nDetails:\n {job_status.details}",
                "public": False
            }
        }

        zenpy_client.tickets.update(ticket_id, **update)

    else:
        update = {
            'comment': {
                'body': f"User upload failed.\nDetails:\n {job_status.details}",
                "public": False
            }
        }
        zenpy_client.tickets.update(ticket_id, **update)