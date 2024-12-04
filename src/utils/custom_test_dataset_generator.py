import json
import os


def extract_first_n_records_optimized(input_file, output_file, n=50):
    try:
        # Resolve and print absolute paths for debugging
        input_file_abs = os.path.abspath(input_file)
        output_file_abs = os.path.abspath(output_file)
        print(f"Input file: {input_file_abs}")
        print(f"Output file: {output_file_abs}")

        # Initialize counters
        count = 0

        # Open input and output files
        with open(input_file, 'r') as infile, open(output_file, 'w') as outfile:
            for line in infile:
                try:
                    # Parse JSON line
                    record = json.loads(line)

                    # Write record to the output file
                    outfile.write(json.dumps(record) + '\n')
                    count += 1

                    # Stop once we've written n records
                    if count >= n:
                        break
                except json.JSONDecodeError as e:
                    print(f"Skipping invalid JSON line: {e}")

        print(f"Successfully extracted {count} records to {output_file}")

    except FileNotFoundError:
        print(f"The file {input_file} does not exist.")
    except Exception as e:
        print(f"An error occurred: {e}")


# Specify the input and output file paths
input_file = '../datasets/yelp_dataset/yelp_academic_dataset_review.json'
output_file = '../datasets/yelp_dataset/test_yelp_academic_dataset_review.json'

# Call the function to extract 100 records
extract_first_n_records_optimized(input_file, output_file, n=100)