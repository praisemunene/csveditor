from django.shortcuts import render, redirect
from django.http import JsonResponse
from django.http import JsonResponse
from django.core.files.base import ContentFile
from django.utils import timezone
import csv
import io
import pandas as pd
from django.http import HttpResponse
from django.views.decorators.csrf import csrf_exempt
import zipfile

def index(request):
    return render(request, 'index.html')
def merge(request):
    return render(request, 'merge.html')

@csrf_exempt
def removefiltered(request):
    if request.method == 'POST':
        # Get the row title and parameter from the form data
        row_title = request.POST.get('row_title', '')
        parameters = request.POST.get('parameter', '').split(',')  # Split parameters by comma

        # Handle the file upload
        uploaded_file = request.FILES.get('file')
        if uploaded_file:
            # Process the uploaded file
            file_content = uploaded_file.read().decode('utf-8')

            # Parse the CSV data
            csv_data = io.StringIO(file_content)
            reader = csv.reader(csv_data)

            # Filter the rows based on the parameters in the given row
            filtered_rows = []
            header = next(reader)
            row_title_index = header.index(row_title)
            for row in reader:
                for parameter in parameters:
                    if row[row_title_index].startswith(parameter.strip()):
                        filtered_rows.append(row)
                        break  # Break out of inner loop if matched to next parameter

            # Create a new CSV file with the filtered rows
            output_csv = io.StringIO()
            writer = csv.writer(output_csv)
            writer.writerow(header)
            writer.writerows(filtered_rows)
            output_csv.seek(0)

            # Prepare response with the new CSV file as attachment
            response = HttpResponse(output_csv.getvalue(), content_type='text/csv')
            response['Content-Disposition'] = 'attachment; filename="filtered_data.csv"'
            return response

    # If the request method is not POST or file processing fails,
    # return a method not allowed response or an error message
    return HttpResponse("Error processing file or invalid request", status=400)


@csrf_exempt
def savefiltered(request):
    if request.method == 'POST':
        # Get the row title and parameter from the form data
        row_title = request.POST.get('row_title', '')
        parameters = request.POST.get('parameter', '').split(',')  # Split parameters by comma

        # Handle the file upload
        uploaded_file = request.FILES.get('file')
        if uploaded_file:
            # Process the uploaded file
            file_content = uploaded_file.read().decode('utf-8')

            # Parse the CSV data
            csv_data = io.StringIO(file_content)
            reader = csv.reader(csv_data)

            # Filter the rows based on the parameters in the given row
            filtered_rows = []
            header = next(reader)
            row_title_index = header.index(row_title)
            for row in reader:
                if not any(row[row_title_index].startswith(parameter.strip()) for parameter in parameters):
                    filtered_rows.append(row)

            # Create a new CSV file with the filtered rows
            output_csv = io.StringIO()
            writer = csv.writer(output_csv)
            writer.writerow(header)
            writer.writerows(filtered_rows)
            output_csv.seek(0)

            # Prepare response with the new CSV file as attachment
            response = HttpResponse(output_csv.getvalue(), content_type='text/csv')
            response['Content-Disposition'] = 'attachment; filename="remaining_data.csv"'
            return response

    # If the request method is not POST or file processing fails,
    # return a method not allowed response or an error message
    return HttpResponse("Error processing file or invalid request", status=400)


@csrf_exempt
def split(request):
    if request.method == 'POST':
        # Get the uploaded file and number of rows per file from the request
        uploaded_file = request.FILES.get('file')
        num_rows_per_file = int(request.POST.get('num_rows', 0))

        if uploaded_file and num_rows_per_file > 0:
            # Process the uploaded file
            file_content = uploaded_file.read().decode('utf-8')

            # Parse the CSV data
            csv_data = io.StringIO(file_content)
            reader = csv.reader(csv_data)
            header = next(reader)

            # Split the CSV data into chunks of specified number of rows
            file_chunks = []  # Initialize the list of chunks
            current_chunk = [header]  # Initialize the current chunk with header row
            row_count = 0

            for row in reader:
                current_chunk.append(row)
                row_count += 1

                # Check if the current chunk size reaches the specified number of rows per file
                if row_count >= num_rows_per_file:
                    file_chunks.append(current_chunk)  # Append the current chunk to the list of chunks
                    current_chunk = []  # Reset the current chunk
                    row_count = 0

            # Append the remaining rows to the last chunk
            if current_chunk:
                file_chunks.append(current_chunk)

            # Create a ZIP file containing the split files
            zip_buffer = io.BytesIO()
            with zipfile.ZipFile(zip_buffer, 'a', zipfile.ZIP_DEFLATED, False) as zip_file:
                for index, chunk in enumerate(file_chunks):
                    # Create a new CSV file in the ZIP archive for each chunk
                    chunk_csv = io.StringIO()
                    writer = csv.writer(chunk_csv)
                    writer.writerows(chunk)

                    # Write the CSV data to the ZIP file
                    zip_file.writestr(f'file_{index + 1}.csv', chunk_csv.getvalue())

            # Prepare response with the ZIP file as attachment
            response = HttpResponse(zip_buffer.getvalue(), content_type='application/zip')
            response['Content-Disposition'] = 'attachment; filename="split_files.zip"'
            return response

    # If the request method is not POST or file processing fails,
    # return a method not allowed response or an error message
    return HttpResponse("Error processing file or invalid request", status=400)

@csrf_exempt
def mergefiles(request):
    if request.method == 'POST' and request.FILES.getlist('files'):
        # Get the list of uploaded files
        uploaded_files = request.FILES.getlist('files')
        
        # Initialize a list to store the content of each CSV file
        csv_content = []

        # Read the content of each CSV file and store it in the csv_content list
        for file in uploaded_files:
            try:
                decoded_file = file.read().decode('utf-8')
                csv_content.append(decoded_file.splitlines())
            except Exception as e:
                return JsonResponse({'error': str(e)}, status=400)
        
        # Merge the CSV content
        merged_csv_content = []
        for content in csv_content:
            csv_reader = csv.reader(content)
            for row in csv_reader:
                merged_csv_content.append(row)

        # Return the merged CSV content as a JSON response
        return JsonResponse({'merged_csv_content': merged_csv_content})

    return JsonResponse({'error': 'No files were uploaded'}, status=400)
