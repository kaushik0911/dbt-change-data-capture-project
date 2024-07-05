import pandas as pd

chunk_size = 100_000  # Adjust based on your memory capacity
sample_size = 1000
sampled_chunks = []
file_path = '/Users/kaushikshamantha/Documents/datasets/Parking_Violations_Issued_-_Fiscal_Year_2023_20231208.csv'
i = 0
for iterator, chunk in enumerate(pd.read_csv(file_path, chunksize=chunk_size)):
    sampled_chunk = chunk.sample(n=min(len(chunk), sample_size))
    sampled_chunks.append(sampled_chunk)
    if iterator == 10:
        break

# Concatenate all sampled chunks
sample_df = pd.concat(sampled_chunks)

# Save the sample to a new CSV file
sample_df.to_csv('sample_data.csv', index=False)

print("Sample saved to 'sample_data.csv'")
