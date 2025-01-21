#!/bin/bash

# Function to rename files in the specified directory to ensure _rX_ becomes _r0X_ only for single-digit numbers
rename_files() {
  local dir_path="$1"
  if [[ ! -d "$dir_path" ]]; then
    echo "Error: Directory $dir_path does not exist."
    return 1
  fi

  for file in "$dir_path"/*; do
    if [[ "$file" =~ _r([0-9])_ ]]; then
      new_name=$(echo "$file" | sed -E 's/_r([0-9])_/_r0\1_/')
      mv "$file" "$new_name"
      echo "Renamed: $file -> $new_name"
    fi
  done
}

rename_files "$1"
# Example usage
# Pass the directory path as an argument to the function
# rename_files /path/to/directory