

import time
import pandas as pd
import yfinance as yf
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from threading import Thread
from fastapi.responses import JSONResponse
import boto3
from io import StringIO
import os
import json

# FastAPI setup

AWS_ACCESS_KEY_ID = "AKIAQXUIX6ZL45CWOUEG"
AWS_SECRET_ACCESS_KEY = "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"
AWS_DEFAULT_REGION = "us-east-1"

os.environ["AWS_ACCESS_KEY_ID"] = AWS_ACCESS_KEY_ID
os.environ["AWS_SECRET_ACCESS_KEY"] = AWS_SECRET_ACCESS_KEY
os.environ["AWS_DEFAULT_REGION"] = AWS_DEFAULT_REGION


app = FastAPI()

# Progress dictionary to track the progress of calculations
progress_dict = {}

# Pydantic model for input
class StartRequest(BaseModel):
    user_id: str
    url: str

# Function to calculate profit/loss for an investment in a given ticker
def calculate_profit(ticker, start_date, end_date, investment_amount):
    try:
        # Download the historical data for the given date range
        data = yf.download(ticker, start=start_date, end=end_date)
        
        # Ensure that we have data for both start and end dates
        if data.empty:
            return 0  # If no data, return no profit or loss
        
        # Get the first and last closing prices (ensuring they are single values)
        start_price = data['Close'].iloc[0]
        end_price = data['Close'].iloc[-1]
        
        # Calculate the number of units bought with the initial investment
        units_bought = investment_amount / start_price
        
        # Calculate the final value of the investment
        final_value = units_bought * end_price
        
        # Calculate the profit or loss
        profit_loss = final_value - investment_amount
        return profit_loss.item()
    except Exception as e:
        # Handle any error, e.g., network issue, invalid data, etc.
        return 0

# Function to track progress and calculate profits for all rows
def process_investments(user_id, df):
    total_rows = len(df)
    total_invested = 0
    stat_pl =0
    nifty_pl_cal =0
    sp500_pl_cal  =0
    company_pl_cal=0

    # Initialize progress_dict entry for this user
    progress_dict[user_id] = {
        'processed': 0,  # This will be the percentage processed
        'total': total_rows,
        'total_invested': 0,
        'stat_pl': 0,
        'company_pl_cal': 0,
        'nifty_pl_cal': 0,
        'sp500_pl_cal': 0,
         'stat_pl_percent': 0,
        'company_pl_cal_percent': 0,
        'nifty_pl_cal_percent': 0,
        'sp500_pl_cal_percent': 0
    }

    for index, row in df.iterrows():
        # Calculate profit/loss for the company's stock
        company_pl = calculate_profit(row['company'], row['buy_date'], row['sell_date'], row['Invested Amount'])
        
        # Calculate profit/loss for Nifty 50 and S&P 500
        nifty_pl= calculate_profit('^NSEI', row['buy_date'], row['sell_date'], row['Invested Amount'])
        sp500_pl = calculate_profit('^GSPC', row['buy_date'], row['sell_date'], row['Invested Amount'])
        
        df.at[index, 'company_pl'] = company_pl
        df.at[index, 'nifty_pl'] = nifty_pl
        df.at[index, 'sp500_pl'] = sp500_pl
        #  # Add the profit/loss values as new columns without changing the original names
        row['company_pl'] = company_pl
        row[ 'nifty_pl'] = nifty_pl
        row['sp500_pl'] = sp500_pl

        print(df.head())
        print("****************")
        # Update the total invested amount and total profit/loss
        total_invested += row['Invested Amount']
        stat_pl += row["Profit/Loss"]
        nifty_pl_cal += row["nifty_pl"]
        sp500_pl_cal  += row["sp500_pl"]
        company_pl_cal += row["company_pl"]

        # Calculate the percentage processed
        progress_dict[user_id]['processed'] = ((index + 1) / total_rows) * 100
        progress_dict[user_id]['total_invested'] = total_invested
        progress_dict[user_id]['stat_pl'] = stat_pl
        progress_dict[user_id]['stat_pl_percent'] = (stat_pl / total_invested) * 100 if total_invested != 0 else 0
        progress_dict[user_id]['nifty_pl_cal'] = nifty_pl_cal
        progress_dict[user_id]['nifty_pl_cal_percent'] = (nifty_pl_cal / total_invested)
        progress_dict[user_id]['sp500_pl_cal'] = sp500_pl_cal
        progress_dict[user_id]['sp500_pl_cal_percent'] = (sp500_pl_cal / total_invested)
        progress_dict[user_id]['company_pl_cal'] = company_pl_cal
        progress_dict[user_id]['company_pl_cal_percent'] = (company_pl_cal / total_invested)
        
        print(progress_dict)
        # Simulate a delay to avoid blocking the server for too long
        time.sleep(0.1)

    # Mark as completed
    progress_dict[user_id]['processed'] = 100
    # progress_dict[user_id]['pl_percent'] = (total_pl / total_invested) * 100 if total_invested != 0 else 0
    # Create a CSV buffer to save the data
    output = StringIO()
    df.to_csv(output, index=False)  # Write DataFrame to the buffer
    output.seek(0)  # Go to the start of the buffer to prepare for upload

    # Save the CSV to S3
    s3_client = boto3.client('s3')
    bucket_name = 'payvin'
    folder_name = user_id  # Use the user_id as the folder name
    file_name = 'metrics.csv'  # Name the file 'metrics.csv'
    
    try:
        # Upload the CSV buffer to S3
        s3_client.put_object(
            Bucket=bucket_name,
            Key=f'{folder_name}/{file_name}',  # Path to save the file in S3
            Body=output.getvalue(),  # The actual content to upload
            ContentType='text/csv'  # Specify the content type as CSV
        )
        print(f"CSV file uploaded successfully to s3://{bucket_name}/{folder_name}/{file_name}")
    except Exception as e:
        print(f"Error uploading to S3: {e}")



@app.post("/start")
async def start_processing(request: StartRequest):
    user_id = request.user_id
    url = request.url

    # Load the CSV data from the URL
    try:
        df = pd.read_csv(url)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to load data: {str(e)}")

    # Start the investment processing in a separate thread to avoid blocking the main thread
    processing_thread = Thread(target=process_investments, args=(user_id, df))
    processing_thread.start()

    return JSONResponse(content={"message": "Data processing started", "user_id": user_id})

@app.get("/{user_id}")
async def get_progress(user_id: str):
    """Get the progress of data processing for a given user_id."""
    if user_id in progress_dict:
        progress = progress_dict[user_id]

        # Ensure NaN values are handled gracefully for JSON serialization
        # return {
        #     "user_id": user_id,
        #     "processed": progress['processed'] if progress['processed'] is not None else 0,
        #     "total": progress['total'],
        #     "percentage_processed": progress['processed'] if progress['processed'] is not None else 0,
        #     "total_invested": progress['total_invested'] if progress['total_invested'] is not None else 0,
        #     "total_pl": progress['total_pl'] if progress['total_pl'] is not None else 0,
        #     "pl_percent": progress['pl_percent'] if progress['pl_percent'] is not None else 0
        # }
        return progress
    else:
        raise HTTPException(status_code=404, detail="No processing found for user_id")










# Define the S3 bucket and folder structure
BUCKET_NAME = "payvin"

# Pydantic model for request body
class UserRequest(BaseModel):
    user_id: str
    till_date: str
s3 = boto3.client('s3')
# Check if file exists in S3 bucket
def check_file_in_s3(user_id: str) -> bool:
    try:
        # Checking if the file exists in the user's folder
        response = s3.list_objects_v2(
            Bucket=BUCKET_NAME,
            Prefix=f"{user_id}/metrics.csv"
        )
        # If the response has Contents, the file exists
        return 'Contents' in response and len(response['Contents']) > 0
    except NoCredentialsError:
        raise HTTPException(status_code=500, detail="AWS credentials not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error checking S3: {str(e)}")

# Function to get the DataFrame from the S3 file
def get_metrics_from_s3(user_id: str) -> pd.DataFrame:
    try:
        # Download file from S3 into memory
        obj = s3.get_object(Bucket=BUCKET_NAME, Key=f"{user_id}/metrics.csv")
        # Read the content into a pandas DataFrame
        df = pd.read_csv(obj['Body'])
        return df
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading metrics from S3: {str(e)}")

@app.post("/get_metrics")
async def get_metrics(user_request: UserRequest):
    user_id = user_request.user_id
    till_date = user_request.till_date
    # Check if metrics.csv exists for the user
    if not check_file_in_s3(user_id):
        return {"message": "Please wait, metrics creation is still in progress."}
    
    # If file exists, get the DataFrame and return the head
    df = get_metrics_from_s3(user_id)
    df = df[df['sell_date'] <= till_date]
    # Compute the required metrics
    total_invested = df["Invested Amount"].sum()
    total_profit_loss = df["Profit/Loss"].sum()
    total_money_after_pl = total_invested + total_profit_loss

    profit_loss_percent = (total_profit_loss / total_invested) * 100
    nifty_pl_percent = (df["nifty_pl"].sum() / total_invested) * 100
    sp500_pl_percent = (df["sp500_pl"].sum() / total_invested) * 100
    company_pl_percent = (df["company_pl"].sum() / total_invested) * 100

    # Print the results
    print(f"Total Invested Money: ₹{total_invested:.2f}")
    print(f"Total Profit/Loss: ₹{total_profit_loss:.2f}")
    print(f"Total Money After Profit/Loss: ₹{total_money_after_pl:.2f}")
    print(f"Profit/Loss Percentage: {profit_loss_percent:.2f}%")
    print(f"NIFTY_PL Percentage: {nifty_pl_percent:.2f}%")
    print(f"SP500_PL Percentage: {sp500_pl_percent:.2f}%")
    print(f"Company PL Percentage: {company_pl_percent:.2f}%")

    df_grouped = df.groupby("company").sum(numeric_only=True).round(2)
    #df_grouped.drop(columns=["column1", "column2"], inplace=True
    df_grouped = df_grouped[['Invested Amount', 'Profit/Loss', 'company_pl', 'nifty_pl', 'sp500_pl']]
    # Display the first few rows of the grouped DataFrame
    # Create percentage columns in df_grouped
    df_grouped["Profit/Loss %"] = (df_grouped["Profit/Loss"] / df_grouped["Invested Amount"]) * 100
    df_grouped["Company PL %"] = (df_grouped["company_pl"] / df_grouped["Invested Amount"]) * 100
    df_grouped["NIFTY PL %"] = (df_grouped["nifty_pl"] / df_grouped["Invested Amount"]) * 100
    df_grouped["SP500 PL %"] = (df_grouped["sp500_pl"] / df_grouped["Invested Amount"]) * 100

    # Round all values to 2 decimal places
    df_grouped = df_grouped.round(2)

    df_grouped_json = df_grouped.to_dict(orient="index")

    print(df_grouped.head(50))






    df_graph = df.groupby("sell_date").sum(numeric_only=True).round(2)
    df_graph = df_graph[['Invested Amount', 'Profit/Loss', 'company_pl', 'nifty_pl', 'sp500_pl']]
    df_graph = df_graph.round(2)
    df_graph["Profit/Loss %"] = (df_graph["Profit/Loss"] / df_graph["Invested Amount"]) * 100
    df_graph["Company PL %"] = (df_graph["company_pl"] / df_graph["Invested Amount"]) * 100
    df_graph["NIFTY PL %"] = (df_graph["nifty_pl"] / df_graph["Invested Amount"]) * 100
    df_graph["SP500 PL %"] = (df_graph["sp500_pl"] / df_graph["Invested Amount"]) * 100
    df_graph = df_graph.round(2)

    df_graph_cumulative = df_graph[['Invested Amount', 'Profit/Loss', 'company_pl', 'nifty_pl', 'sp500_pl']].cumsum()

    # Recalculate percentages based on cumulative invested amount
    df_graph_cumulative["Profit/Loss %"] = (df_graph_cumulative["Profit/Loss"] / df_graph_cumulative["Invested Amount"]) * 100
    df_graph_cumulative["Company PL %"] = (df_graph_cumulative["company_pl"] / df_graph_cumulative["Invested Amount"]) * 100
    df_graph_cumulative["NIFTY PL %"] = (df_graph_cumulative["nifty_pl"] / df_graph_cumulative["Invested Amount"]) * 100
    df_graph_cumulative["SP500 PL %"] = (df_graph_cumulative["sp500_pl"] / df_graph_cumulative["Invested Amount"]) * 100

    # Round values to 2 decimal places
    df_graph_cumulative = df_graph_cumulative.round(2)
    df_graph_json = df_graph_cumulative.to_dict(orient="index")


    output_json = {
    "total_invested": round(total_invested, 2),
    "total_profit_loss": round(total_profit_loss, 2),
    "total_money_after_pl": round(total_money_after_pl, 2),
    "profit_loss_percent": round(profit_loss_percent, 2),
    "nifty_pl_percent": round(nifty_pl_percent, 2),
    "sp500_pl_percent": round(sp500_pl_percent, 2),
    "company_pl_percent": round(company_pl_percent, 2),
    "data": df_grouped_json,
    "graph": df_graph_json
    }

    # Convert to JSON string
    #json_output = json.dumps(output_json, indent=4)


    return output_json



if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
