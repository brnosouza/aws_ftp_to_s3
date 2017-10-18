import base64

with open('trilha01.mp3', 'rb') as file_input:
    with open('trilla01.txt', 'wb') as file_output:
        file_output.write(file_input.read())