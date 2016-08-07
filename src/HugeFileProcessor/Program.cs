using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime;
using System.Text;
using System.Threading.Tasks;

namespace HugeFileProcessor
{
    internal sealed class Program
    {
        private static readonly char[] STR_SPLIT = new char[] { ' ' };

        static void Main(string[] args)
        {
            if (args.Length > 0)
            {
                switch (args[0])
                {
                case "-shuffle":
                    MainShuffle(args[1], Int32.Parse(args[2]), args.Length == 4 ? args[3] : args[1] + ".shuffled");
                    return;
                case "-split":
                    {
                        string[] splitFracStr = args[2].Split('/');     //a fraction of test data in form "1/10". To skip creation of test data, use "0/1"
                        int testFracUp = Int32.Parse(splitFracStr[0]);
                        int testFracDown = Int32.Parse(splitFracStr[1]);
                        int processLinesLimit = args.Length == 4 ? Int32.Parse(args[3]) : -1;   //number of lines to process from file or -1 if all.

                        MainSplit(args[1], testFracUp, testFracDown, processLinesLimit);
                    }
                    return;
                }
            }
            Console.WriteLine("Usage :");
            Console.WriteLine("-split <sourceFile> <test>/<base> [<linesLimit>]\n\tsplits <sourceFile> to test and train, so that test file get (<test>/<base>) fraction of lines.\n\tSet 0 to <test> to skip test file creation.\n\t<linesLimit> - total number of lines to proces from <sourceFile>, set to -1 or skip to read all lines.\n\n");
            Console.WriteLine("-shuffle <sourceFile> <batchSize> [<outFile>]\n\tshuffles lines from <sourceFile> to <outFile>.\n\t<batchSize> is in lines.\n\n");
            Environment.Exit(1);
        }

        #region shuffle
        private static void MainShuffle(string sourceFileName, int batchSizeLines, string targetFileName)
        {
            TimeSpan singlePassTime;
            int linesCount = CountLines(sourceFileName, out singlePassTime);
            double batchesCount = linesCount * 1.0 / batchSizeLines;
            Console.WriteLine();
            Console.WriteLine($"Expecting {batchesCount:N3} batches, that would take {TimeSpan.FromSeconds(Math.Ceiling(batchesCount) * singlePassTime.TotalSeconds)}");
            Console.WriteLine();

            int[] orderArray = GetOrderArray(linesCount);
            Console.WriteLine();

            Console.WriteLine("Writing to file");
            DateTime start = DateTime.UtcNow;
            using (StreamWriter writer = new StreamWriter(targetFileName))
            {
                int batchIndex = 0;
                for (int batchStart = 0; batchStart < linesCount; batchStart += batchSizeLines)
                {
                    ++batchIndex;
                    Console.WriteLine($"Starting batch {batchIndex}");
                    int batchEnd = batchStart + batchSizeLines - 1;
                    if (batchEnd >= linesCount)
                    {
                        batchEnd = linesCount - 1;
                    }
                    ProcessBatch(sourceFileName, orderArray, batchStart, batchEnd, writer);
                    TimeSpan took = DateTime.UtcNow - start;
                    Console.WriteLine($"Batch done, took {took}, speed {batchEnd / took.TotalSeconds:N0} lps. Remaining {TimeSpan.FromSeconds((batchesCount - batchIndex) * took.TotalSeconds / batchIndex)}");
                    Console.WriteLine();
                }
            }
            Console.WriteLine($"Done, took {DateTime.UtcNow - start}");
        }

        private static void ProcessBatch(string sourceFileName, int[] orderArray, int batchStart, int batchEnd, StreamWriter writer)
        {
            int batchSize = batchEnd - batchStart + 1;
            KeyValuePair<int, int>[] batchLines = new KeyValuePair<int, int>[batchEnd - batchStart + 1];
            for (int i = 0; i < batchSize; ++i)
            {
                batchLines[i] = new KeyValuePair<int, int>(orderArray[batchStart + i], i);
            }
            Array.Sort(batchLines, (a, b) => a.Key.CompareTo(b.Key));


            string[] writeLines = new string[batchSize];


            using (StreamReader reader = File.OpenText(sourceFileName))
            {
                int lineIndex = -1;
                foreach (KeyValuePair<int, int> pair in batchLines)
                {
                    string s = null;
                    while (lineIndex < pair.Key)
                    {
                        s = reader.ReadLine();
                        ++lineIndex;
                    }
                    writeLines[pair.Value] = s;
                }
            }

            foreach (string writeLine in writeLines)
            {
                writer.WriteLine(writeLine);
            }

            RunLOHDefragmentation();
        }

        private static int CountLines(string fileName, out TimeSpan totalTime)
        {
            Console.WriteLine("Counting lines");
            DateTime start = DateTime.UtcNow;

            int linesCount = 0;
            //using (StreamReader sr = File.OpenText(fileName))
            {

                //string s = String.Empty;
                //while ((s = sr.ReadLine()) != null)
                foreach (string s in File.ReadLines(fileName))
                {
                    if (string.IsNullOrWhiteSpace(s))
                    {
                        continue;
                    }
                    ++linesCount;

                    if (linesCount % 100000 == 0)
                    {
                        TimeSpan took = DateTime.UtcNow - start;
                        Console.WriteLine($"Current count is {linesCount:N0}, took {took}, speed is {linesCount / took.TotalSeconds:N0} lps");
                    }
                }
            }
            totalTime = DateTime.UtcNow - start;
            Console.WriteLine($"Done. Lines count is {linesCount:N0}, took {totalTime}, speed {linesCount / totalTime.TotalSeconds:N0} lps");


            return linesCount;
        }

        private static int[] GetOrderArray(int linesCount)
        {
            Console.WriteLine("Creating order array");
            DateTime start = DateTime.UtcNow;

            int[] orderArray = new int[linesCount];
            Random rnd = new Random();
            for (int i = 0; i < linesCount; ++i)
            {
                orderArray[i] = i;
            }
            for (int i = 0; i < linesCount - 1; ++i)
            {
                int j = i + rnd.Next(linesCount - i);
                int tmp = orderArray[i];
                orderArray[i] = orderArray[j];
                orderArray[j] = tmp;
            }
            Console.WriteLine($"Done, took {DateTime.UtcNow - start}");
            return orderArray;
        }
        #endregion

        #region split
        private static void MainSplit(string sourceFileName, int testFracUp, int testFracDown, int processLinesLimit)
        {
            string outFileInfix = "";
            if (processLinesLimit < 0)
            {
                processLinesLimit = Int32.MaxValue;
            }
            else
            {
                outFileInfix = "." + processLinesLimit.ToString();
            }

            using (StreamWriter trainWriter = new StreamWriter(sourceFileName + outFileInfix + ".train"))
            {
                using (StreamWriter testWtier = testFracUp <= 0 ? null : new StreamWriter(sourceFileName + outFileInfix + ".test"))
                {
                    int lineIndex = 0;

                    //int elementsCount = -1;
                    foreach (string sourceLine in File.ReadLines(sourceFileName))
                    {
                        ++lineIndex;
                        /*
                        string[] elements = sourceLine.Split(STR_SPLIT);
                        if (elementsCount < 0)
                        {
                            elementsCount = elements.Length;
                            Console.WriteLine($"Elements count is {elementsCount}");
                        }
                        else if (elementsCount != elements.Length)
                        {
                            Console.WriteLine($"Elements count failed on line {lineIndex}. Stopping");
                            break;
                        }
                        if (sourceLine.Contains("  "))
                        {
                            Console.WriteLine($"Weird two-space found on line {lineIndex}. Stopping");
                            break;
                        }
                        */
                        if ((lineIndex - 1) % testFracDown < testFracUp)
                        {
                            testWtier.WriteLine(sourceLine);
                        }
                        else
                        {
                            trainWriter.WriteLine(sourceLine);
                        }
                        if (lineIndex % 10000 == 0)
                        {
                            Console.WriteLine($"Processed {lineIndex} lines");
                        }
                        if (lineIndex >= processLinesLimit)
                        {
                            Console.WriteLine($"Processed up to limit. Stopping");
                            break;
                        }
                    }
                    Console.WriteLine($"Finished. Processed {lineIndex} lines");
                }
            }
        }
        #endregion

        public static void RunLOHDefragmentation()
        {
            DateTime started = DateTime.UtcNow;
            Console.WriteLine($"Collecting and defragging LOH... ");

            GCSettings.LargeObjectHeapCompactionMode = GCLargeObjectHeapCompactionMode.CompactOnce;

            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.WaitForFullGCComplete();
            GC.Collect();

            Console.WriteLine($"Collecting and defragging LOH... done in {DateTime.UtcNow - started}");
        }

    }
}
