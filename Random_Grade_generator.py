"""
    This program generates a random data set containing student ID and random grades for 5 different classes.
    Author: Amelia Teare
    Date: October 5, 2023

"""

import random
import csv

def generate_grades():
    return[random.choice(['4', '3.5', '3', '2.5', '2', '1.5', '1', '0.5', '0']) for _ in range(5)]

data = []
num_students = 100

for student_id in range(1, num_students + 1):
    grades = generate_grades()
    data.append([student_id] + grades)

with open('grades.csv', 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(['StudentID', 'Math', 'Science', 'English', 'Social Studies', 'Reading'])
    csvwriter.writerows(data)

print(f"Random data for {num_students} students has been generated and saved to 'grades.csv'.")
