import io
import zlib
import sys
import os
import struct
import time

"""
Unpacker and packer for Midnight Club 3 .dat archives.
Files can be packed raw or compressed.
Stringtable is always raw (not base64 encoded).
"""


BASE64CHARSET = f"\x00 #$()-./?0123456789_abcdefghijklmnopqrstuvwxyz~"

def Padding(num, pad=0x800, restOnly=False) -> int:
    if restOnly:
        return 0 if (num % pad) == 0 else pad - (num % pad)
    return num if (num % pad) == 0 else num + pad - (num % pad)

class TOCEntry:
    def __init__(self, file):
        self.nameOffset = int.from_bytes(file.read(4), "little")
        self.fileOffset = int.from_bytes(file.read(4), "little")
        self.uncompressedSize = int.from_bytes(file.read(4), "little")
        self.compressedSize = int.from_bytes(file.read(4), "little")

        self.filepath = ""

    def isCompressed(self):
        return True if self.uncompressedSize != self.compressedSize else False
    
    def getDir(self):
        path = self.filepath.split("/")
        return "/".join(path[:-1]) if len(path) > 1 else ""
    
    def isDir(self):
        return True if self.filepath.endswith("/") else False


def unpack(file, outputDirectory: str):
    magic = file.read(4).decode("ANSI")
    numFiles = int.from_bytes(file.read(4), "little")
    tocLength = int.from_bytes(file.read(4), "little")
    stringTableLength = int.from_bytes(file.read(4), "little")

    file.seek(0x800)

    fileEntries = [TOCEntry(file) for amount in range(numFiles)]

    file.seek(0x800 + tocLength)

    stringTable = io.BytesIO(file.read(stringTableLength))

    if magic == "Dave": #String table is base64 encoded with a custom Charset (little-endian)

        prevName = ""
        for entry in fileEntries:
            temp = []
            remainingBits = lastValue = 0

            stringTable.seek(entry.nameOffset)

            while True:
                value = int.from_bytes(stringTable.read(1), "little") if remainingBits != 6 else 0
                temp.append(((value & ((2**(6-remainingBits))-1)) << remainingBits) | (lastValue >> (8-remainingBits)))
                remainingBits = 0 if remainingBits == 6 else 8 - (6-remainingBits)
                lastValue = value
                if temp[-1] == 0:
                    break

            if temp[0] < 56:
                entry.filepath = "".join(BASE64CHARSET[char] for char in temp[:-1] if char < 48)
            else:
                entry.filepath = prevName[:(temp[0]-0x38) + ((temp[1]-0x20) << 3)] + "".join(BASE64CHARSET[char] for char in temp[2:-1] if char < 48)
            prevName = entry.filepath

    elif magic == "DAVE": #String table is not base64 encoded (raw null-terminated strings)

        for entry in fileEntries:
            stringTable.seek(entry.nameOffset)

            while True:
                byte = stringTable.read(1)
                if byte == b"\x00":
                    break
                entry.filepath += byte.decode("ANSI")

    for index, entry in enumerate(fileEntries):   
        if os.path.exists(outputDirectory) == False:
            raise Exception("Output directory does not exist")
                            
        filepath = os.path.join(outputDirectory, entry.filepath).replace("/","\\")
        dirpath = os.path.join(outputDirectory, entry.getDir()).replace("/","\\")

        if os.path.exists(dirpath) == False:
            os.makedirs(dirpath, exist_ok=True)
        if entry.isDir(): 
            continue

        file.seek(entry.fileOffset)

        with open(filepath, "wb") as f:
           if entry.isCompressed():
               f.write(zlib.decompress(file.read(entry.compressedSize), wbits=-15))
           else:
               f.write(file.read(entry.compressedSize))

        if index % 100 == 0:
            print(f"Extracted {index} files. Remaining: {numFiles-index}")


def pack(assetPath: str, outputPath: str, compress: bool):
    stringTable = []

    for root, dirs, files in os.walk(assetPath, topdown=True):
        root = root[len(assetPath):].replace("\\","/")
        for dir in dirs:
            stringTable.append(root + "/" + dir + "/" if root != "" else dir + "/")
        for filepath in files:
            stringTable.append(root + "/" + filepath if root != "" else filepath.replace("\\","/").replace(chr(92),"/"))

    stringTable.sort()

    estimatedTOCSize = Padding(len(stringTable)*16)
    estimatedStringTableSize = Padding(sum(len(path)+1 for path in stringTable))

    Header = io.BytesIO(b"DAVE" + struct.pack("<III", len(stringTable), estimatedTOCSize, estimatedStringTableSize) + bytes(0x7F0))
    TOCBinary = io.BytesIO()
    stringTableBinary = io.BytesIO(b"\x00".join(name.encode("ANSI") for name in stringTable))
    filesBinary = io.BytesIO()

    estimatedFileOffset = 0x800 + estimatedTOCSize + estimatedStringTableSize
    nameOffset = 0
    for index, path in enumerate(stringTable):
        data = b""
        uncompressedSize = compressedSize = 0
        if not path.endswith("/"):
            with open(os.path.join(assetPath, path), "rb") as file:
                data = file.read()
                uncompressedSize = compressedSize = len(data)

            if compress:
                compressor = zlib.compressobj(-1, zlib.DEFLATED, -15)
                data = compressor.compress(data) + compressor.flush()
                compressedSize = len(data)

        TOCBinary.write(struct.pack("<IIII", nameOffset, estimatedFileOffset, uncompressedSize, compressedSize))
        filesBinary.write(data + bytes(Padding(len(data), restOnly=True)))

        nameOffset += len(path) + 1
        estimatedFileOffset += Padding(len(data))

        if index % 500 == 0:
            print(index)

    with open(outputPath, "wb") as file:
        print("Writing Header....")
        file.write(Header.getvalue())

        print("Writing Table of Contents....")
        file.write(TOCBinary.getvalue() + bytes(Padding(TOCBinary.getbuffer().nbytes, restOnly=True)))

        print("Writing String Table....")
        file.write(stringTableBinary.getvalue() + bytes(Padding(stringTableBinary.getbuffer().nbytes, restOnly=True)))

        print("Writing Files....")
        file.write(filesBinary.getvalue())


if __name__ == "__main__":
    try:
        if sys.argv[1].lower() == "unpack":
            with open(sys.argv[2], "rb") as file:
                file.seek(4)
                print(f"Warning: This will unpack {int.from_bytes(file.read(4), 'little')} files. Will continue in 5 seconds.")
                time.sleep(5)
                file.seek(0)
                unpack(file, sys.argv[3])    
        elif sys.argv[1].lower() == "pack":
            if len(sys.argv) == 4:
                pack(sys.argv[2], sys.argv[3], False)
            elif len(sys.argv) == 5 and sys.argv[4] == "compress":
                pack(sys.argv[2], sys.argv[3], True)
    except Exception as e:
        print(e)
    else:
        print("File successfully (un)packed!")


        
        
                
                       
                       

