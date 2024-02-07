import {open, stat} from "node:fs/promises";
import {Heap} from 'heap-js';

// bytes
const RAM_SIZE = Number(process.env.RAM_MB) * Math.pow(1024, 2);

async function* readLines(fileHandle, options) {
    const rl = fileHandle.readLines(options)
    fileHandle.setMaxListeners(50)
    yield* rl
}

function createInputFile(fileHandle) {
    return new Promise(resolve => {
        const wr = fileHandle.createWriteStream();

        let count = RAM_SIZE * 10;

        function write() {
            let ok = true;
            do {
                count--;
                const value = String(Math.floor(Math.random() * 100));

                if (count === 0) {
                    wr.write(value);
                } else {
                    ok = wr.write(value + '\n');
                }
            } while (count > 0 && ok);
            if (count > 0) {
                wr.once('drain', write);
            } else {
                resolve();
            }
        }

        write()
    })
}

async function sortTempFile(fileHandle, tempFile) {
    const fileSize = (await stat('./data/input.txt')).size

    const iterators = []
    let position = 0;
    let buffer = Buffer.alloc(RAM_SIZE)

    while (position < fileSize) {
        const {bytesRead: bufferLength} = await fileHandle.read({
            buffer,
            length: RAM_SIZE,
            position,
        })

        const lastLineIndex = buffer.lastIndexOf('\n');
        const r = (lastLineIndex === -1 || bufferLength < RAM_SIZE) ? bufferLength : lastLineIndex
        const subBuffer = buffer.subarray(0, r)

        await tempFile.writeFile(
            subBuffer
                .toString()
                .split('\n')
                .sort((a, b) => a - b)
                .join('\n') + `\n`);

        iterators.push(readLines(tempFile, {
            start: position,
            end: position + subBuffer.length,
            autoClose: false,
        }))
        position += subBuffer.length + 1;
    }
    await fileHandle.close();
    return iterators
}

async function writeOutputFile(fileHandle, iterators) {
    return new Promise(async resolve => {
        const wr = fileHandle.createWriteStream();
        const minHeap = new Heap((a, b) => a.value - b.value)

        for (let i = 0; i < iterators.length; i++) {
            const {value} = await iterators[i].next();
            minHeap.push({index: i, value})
        }

       async function write() {
            let ok = true;
            while (minHeap.size() && ok) {
                const {value, index} = minHeap.pop();
                ok = wr.write(value + '\n')

                const {done, value: nextValue} = await iterators[index].next();
                if (!done) {
                    minHeap.push({index, value: nextValue})
                }
            }
            if (minHeap.size()) {
                wr.once('drain', write)
            } else {
                resolve();
            }
        }

        await write();
    })
}


async function main() {
    const file = await open('./data/input.txt', 'w+');

    console.log('Создание входного файла...')
    await createInputFile(file);

    const tempFile = await open('./data/temp.txt', 'w+')
    console.log('Сортировка во временный файл...')
    const iterators = await sortTempFile(file, tempFile)
    tempFile.setMaxListeners(iterators.length);

    const resultFile = await open('./data/output.txt', 'w+')
    console.log('Запись в конечный файл...')
    await writeOutputFile(resultFile, iterators);
    await tempFile.close()
    await resultFile.close()
}

main();






