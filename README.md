# HugeFileProcessor
An utility to randomize and split really huge (100+ GB) text files

# Motivation
While doing some Machine Learning stuff, I've stumpled upon a need to process (mostly shuffle and split) **really huge** text files. Where "huge" might mean hundreds of gigabytes. For example the easiest way to feed data to [CNTK](https://github.com/Microsoft/CNTK) is using a text file. I've was amazed that I was not able to find any tool capable of suffling a huge file without loading it whole into RAM. So I wrote my own. Here it is.

# Usage
There are just a few commands that HugeFileProcessor understands:
* **`-shuffle <sourceFile> <batchSize> [<outFile>]`** 
 
Shuffles lines from `sourceFile` into `outFile` (or into `sourceFile`.shuffled if no `outFile` specified).

This mode requires specifying `batchSize` - number of lines to keep in RAM when writing to ouput. The more is the better (unless you are out of RAM), because total shuffling time would be `(number of lines in <sourceFile>) / <batchSize> * (time to fully read <sourceFile>)`. Please note that the program **shuffles whole file**, not on per-batch basis. See the details on shuffling below.

* **`-split <sourceFile> <test>/<base> [<linesLimit>]`**

Splits `sourceFile` into `sourceFile`.test and `sourceFile`.train. .test would get `<test>/<base>` lines of `sourceFile`, and .train would get `(<base>-<test>)/<base>` lines. This is done in a single pass through the `sourceFile`, so it's faster than using `head`/`tail`.

If `linesLimit` is specified, then only first `linesLimit` of `sourceFile` are processed.

If `test` is set to 0, then not .test file is created and all lines get into .train file. When combined with `linesLimit` this is equal to calling `head -n <linesLimit>`.

* **`-count <sourceFile>`**

Just count number of lines in `sourceFile`.

# Running the program

The program is written in C#. It requires .Net 4.5.1 or greater. 

To run on linux use Mono: `mono HugeFileProcessor.exe <options>`

# Shuffling

Here are the details on shuffling implementation. The algorithm is as follows.

1. Count lines in `sourceFile`. This is done simply by reaing whole file line-by-line. (See some comparisons [here](http://cc.davelozinski.com/c-sharp/fastest-way-to-read-text-files).) This also gives a measurement of how much time would it take to read whole file once. So we could estimate how many times it would take to make a complete shuffle because it would require `Ceil(linesCount / batchSize)` complete file reads.

2. As we now know the total `linesCount`, we can create an index array of `linesCount` size and shuffle it using [Fisher–Yates](https://en.wikipedia.org/wiki/Fisher–Yates_shuffle) (called `orderArray` in the code). This would give us an order in which we want to have lines in a shuffled file. Note that this is a global order over the whole file, not per batch or chunk or something.

3. Now the actual code. We need to get all lines from `sourceFile` in a order we just computed, but we can't read whole file in memory. So we just split the task. 
 * We would go through the `sourceFile` reading all lines and storing in memory only those lines that would be in first `batchSize` of the `orderArray`. When we get all these lines, we could write them into `outFile` in required order, and it's a `batchSize`/`linesCount` of work done.
 * Next we would repeat whole process again and again taking next parts of `orderArray` and reading `sourceFile` from start to end for each part. Eventually the whole `orderArray` is processed and we are done.

####Why it works?

Because all we do is just reading the source file from start to end. No seeks forward/backward, and that's what HDDs like. File gets read in chunks according to internal HDD buffers, FS blocks, CPU cahce, etc. and everything is being read sequentially.

####Some numbers
On my machine (Core i5, 16GB RAM, Win8.1, HDD Toshiba DT01ACA200 2TB, NTFS) I was able to shuffle a file of 132 GB (84 000 000 lines) in around 5 hours using `batchSize` of 3 500 000. With `batchSize` of 2 000 000 it took around 8 hours. Reading speed was around 118000 lines per second.

 
