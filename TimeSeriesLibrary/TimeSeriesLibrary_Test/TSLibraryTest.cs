using TimeSeriesLibrary;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Linq;
using System.Collections.Generic;

namespace TimeSeriesLibrary_Test
{
    
    
    /// <summary>
    ///This is a test class for TSLibraryTest and is intended
    ///to contain all TSLibraryTest Unit Tests
    ///</summary>
    [TestClass()]
    [DeploymentItem("lzfx.dll")]
    public class TSLibraryTest
    {
        static List<TimeSeriesValue> IrregList1 = new List<TimeSeriesValue>();
        static List<TimeSeriesValue> IrregList2 = new List<TimeSeriesValue>();
        static List<TimeSeriesValue> RegList1 = new List<TimeSeriesValue>();
            static TSDateCalculator.TimeStepUnitCode TimeStepUnit1 = TSDateCalculator.TimeStepUnitCode.Hour;
            static short TimeStepQuantity1 = 3;
            static DateTime BlobStartDate1 = DateTime.Parse("1/1/1920 12:00 PM");
            static int TimeStepCount1 =500;
        static List<TimeSeriesValue> RegList2 = new List<TimeSeriesValue>();
            static TSDateCalculator.TimeStepUnitCode TimeStepUnit2 = TSDateCalculator.TimeStepUnitCode.Month;
            static short TimeStepQuantity2 = 1;
            static DateTime BlobStartDate2 = DateTime.Parse("3/3/1933 12:11 PM");
            static int TimeStepCount2 = 1144;

        private TestContext testContextInstance;

        #region TestContext
        /// <summary>
        ///Gets or sets the test context which provides
        ///information about and functionality for the current test run.
        ///</summary>
        public TestContext TestContext
        {
            get
            {
                return testContextInstance;
            }
            set
            {
                testContextInstance = value;
            }
        } 
        #endregion


        #region Additional test attributes
        // 
        //You can use the following additional attributes as you write your tests:
        
        //Use ClassInitialize to run code before running the first test in the class
        [ClassInitialize()]
        public static void MyClassInitialize(TestContext testContext)
        {
            int i;

            IrregList1.Add(new TimeSeriesValue { Date = BlobStartDate1, Value = 1.1 });
            RegList1.Add(new TimeSeriesValue{ Date=BlobStartDate1, Value=-500.0 });

            for(i=1; i<TimeStepCount1; i++)
            {
                IrregList1.Add(new TimeSeriesValue{ 
                        Date=IrregList1[i-1].Date.AddHours(Math.Sqrt(i)),
                        Value=IrregList1[i-1].Value + i/4 });
                RegList1.Add(new TimeSeriesValue{
                        Date=RegList1[i-1].Date.AddHours(3),
                        Value=i * 10 });
            }

            IrregList2.Add(new TimeSeriesValue { Date = BlobStartDate2, Value = 3.5 });
            RegList2.Add(new TimeSeriesValue { Date = BlobStartDate2, Value = -20.0 });

            for (i = 1; i < TimeStepCount2; i++)
            {
                IrregList2.Add(new TimeSeriesValue
                {
                    Date = IrregList2[i-1].Date.AddDays(i/2),
                    Value = IrregList2[i-1].Value * 1.222,
                });
                RegList2.Add(new TimeSeriesValue
                {
                    Date = RegList2[i-1].Date.AddMonths(1),
                    Value = 3.0
                });
            }
        }

        #endregion


        #region Test Methods for ConvertBlobToListAll() and ConvertListToBlob()
        // The series of tests below is for ConvertBlobToListAll and ConvertListToBlob.
        // The tests take advantage of the fact that the methods are designed so that
        // the series that is put into the BLOB must be identical to the series that
        // comes out of the BLOB.

        // This method is re-used by the actual test methods that follow.
        public void ConvertBlobAll(List<TimeSeriesValue> inList,
                TSDateCalculator.TimeStepUnitCode timeStepUnit, short timeStepQuantity,
                DateTime blobStartDate)
        {
            TSLibrary tsLib = new TSLibrary();
            List<TimeSeriesValue> outList = new List<TimeSeriesValue>();
            int compressionCode;

            byte[] blobData = tsLib.ConvertListToBlobWithChecksum(timeStepUnit, timeStepQuantity,
                                inList.Count, inList.First().Date, inList.Last().Date, inList,
                                new TSTrace { TraceNumber=1 }, out compressionCode);

            int ret = tsLib.ConvertBlobToListAll(timeStepUnit, timeStepQuantity,
                            inList.Count, blobStartDate, blobData, ref outList, compressionCode);

            // The return value of the function must match the number of items in the original list
            Assert.AreEqual(ret, inList.Count);
            // the count in both lists must match
            Assert.AreEqual(outList.Count, inList.Count);

            // now check each item in the two lists
            Boolean AreEqual = true;
            for (int i = 0; i < ret; i++)
            {
                if (outList[i].ValueEquals(inList[i]) == false)
                    AreEqual = false;
            }

            Assert.IsTrue(AreEqual);
        }
        [TestMethod()]
        public void ConvertBlobToListAllIrregTest1()
        {
            ConvertBlobAll(IrregList1, TSDateCalculator.TimeStepUnitCode.Irregular, 0, BlobStartDate1);
        }
        [TestMethod()]
        public void ConvertBlobToListAllIrregTest2()
        {
            ConvertBlobAll(IrregList2, TSDateCalculator.TimeStepUnitCode.Irregular, 0, BlobStartDate2);
        }
        [TestMethod()]
        public void ConvertBlobToListAllRegTest1()
        {
            ConvertBlobAll(RegList1, TimeStepUnit1, TimeStepQuantity1, BlobStartDate1);
        }
        [TestMethod()]
        public void ConvertBlobToListAllRegTest2()
        {
            ConvertBlobAll(RegList2, TimeStepUnit2, TimeStepQuantity2, BlobStartDate2);
        } 
        #endregion


        #region Test Methods for ConvertBlobToListLimited() and ConvertListToBlob()
        // The series of tests below is for ConvertBlobToListLimited and ConvertListToBlob.
        // The tests take advantage of the fact that the methods are designed so that
        // the series that is put into the BLOB must be identical to the series that
        // comes out of the BLOB.

        // This method is re-used by the actual test methods that follow.
        public void ConvertBlobLimited(List<TimeSeriesValue> inList,
                TSDateCalculator.TimeStepUnitCode timeStepUnit, short timeStepQuantity,
                DateTime blobStartDate,
                int nCutStart,          // the number of time steps that the test will truncate from the start of the series
                int nCutEnd,            // the number of time steps that the test will truncate from the end of the series
                int nMax)               // the maximum number of time steps to put into the series
        {
            TSLibrary tsLib = new TSLibrary();
            List<TimeSeriesValue> outList = new List<TimeSeriesValue>();
            int compressionCode;

            byte[] blobData = tsLib.ConvertListToBlobWithChecksum(timeStepUnit, timeStepQuantity,
                                inList.Count, inList.First().Date, inList.Last().Date, inList,
                                new TSTrace { TraceNumber = 1 }, out compressionCode);

            int ret = tsLib.ConvertBlobToListLimited(timeStepUnit, timeStepQuantity,
                            inList.Count, blobStartDate,
                            nMax, inList[nCutStart].Date, inList[inList.Count - nCutEnd - 1].Date,
                            blobData, ref outList, compressionCode);

            // The return value of the function must match the number of items in the original list
            Assert.AreEqual(ret, Math.Min(nMax, inList.Count - nCutStart - nCutEnd));
            // the count in both lists must match
            Assert.AreEqual(outList.Count, Math.Min(nMax, inList.Count - nCutStart - nCutEnd));

            // now check each item in the two lists
            Boolean AreEqual = true;
            for (int i = 0; i < ret; i++)
            {
                if (outList[i].ValueEquals(inList[i + nCutStart]) == false)
                    AreEqual = false;
            }

            Assert.IsTrue(AreEqual);
        }
        [TestMethod()]
        public void ConvertBlobToListLimitedIrregTest1()
        {
            ConvertBlobLimited(IrregList1, TSDateCalculator.TimeStepUnitCode.Irregular, 0, BlobStartDate1, 3, 5, TimeStepCount1);
        }
        [TestMethod()]
        public void ConvertBlobToListLimitedIrregTest2()
        {
            ConvertBlobLimited(IrregList2, TSDateCalculator.TimeStepUnitCode.Irregular, 0, BlobStartDate2, 5, 13, TimeStepCount2);
        }
        [TestMethod()]
        public void ConvertBlobToListLimitedIrregTest3()
        {
            ConvertBlobLimited(IrregList2, TSDateCalculator.TimeStepUnitCode.Irregular, 0, BlobStartDate2, 0, 100, TimeStepCount2);
        }
        [TestMethod()]
        public void ConvertBlobToListLimitedIrregTest4()
        {
            ConvertBlobLimited(IrregList2, TSDateCalculator.TimeStepUnitCode.Irregular, 0, BlobStartDate2, 100, 0, TimeStepCount2);
        }
        [TestMethod()]
        public void ConvertBlobToListLimitedRegTest1()
        {
            ConvertBlobLimited(RegList1, TimeStepUnit1, TimeStepQuantity1, BlobStartDate1, 4, 7, TimeStepCount1);
        }
        [TestMethod()]
        public void ConvertBlobToListLimitedRegTest2()
        {
            ConvertBlobLimited(RegList2, TimeStepUnit2, TimeStepQuantity2, BlobStartDate2, 7, 4, TimeStepCount2);
        }
        [TestMethod()]
        public void ConvertBlobToListLimitedRegTest3()
        {
            ConvertBlobLimited(RegList2, TimeStepUnit2, TimeStepQuantity2, BlobStartDate2, 0, 180, TimeStepCount2);
        }
        [TestMethod()]
        public void ConvertBlobToListLimitedRegTest4()
        {
            ConvertBlobLimited(RegList2, TimeStepUnit2, TimeStepQuantity2, BlobStartDate2, 180, 0, TimeStepCount2);
        }
        [TestMethod()]
        public void ConvertBlobToListLimitedRegTest5()
        {
            ConvertBlobLimited(RegList1, TimeStepUnit1, TimeStepQuantity1, BlobStartDate1, 0, 180, 72);
        }
        [TestMethod()]
        public void ConvertBlobToListLimitedRegTest6()
        {
            ConvertBlobLimited(RegList2, TimeStepUnit2, TimeStepQuantity2, BlobStartDate2, 180, 0, 231);
        } 
        #endregion


        #region Test Methods for ConvertListToBlobWithChecksum() without errors
        // The series of tests below is for ConvertListToBlobWithChecksum()
        //

        // This method is reused by the actual test methods that follow
        public Boolean ComputeTestChecksums(
                TSDateCalculator.TimeStepUnitCode u1, short q1, List<TimeSeriesValue> list1, ITimeSeriesTrace trace1,
                TSDateCalculator.TimeStepUnitCode u2, short q2, List<TimeSeriesValue> list2, ITimeSeriesTrace trace2)
        {
            TSLibrary tsLib = new TSLibrary();
            int compressionCode;

            tsLib.ConvertListToBlobWithChecksum(
                u1, q1, list1.Count,
                list1[0].Date, list1[list1.Count - 1].Date, list1, trace1, out compressionCode);

            tsLib.ConvertListToBlobWithChecksum(
                u2, q2, list2.Count,
                list2[0].Date, list2[list2.Count - 1].Date, list2, trace2, out compressionCode);

            Assert.IsTrue(trace1.Checksum.Length == 16);
            Assert.IsTrue(trace2.Checksum.Length == 16);

            for (int i = 0; i < trace2.Checksum.Length; i++)
                if (trace1.Checksum[i] != trace2.Checksum[i])
                    return false;

            return true;

        }
        // Test whether identical checksums are returned from identical timeseries.
        // Note that this test seems trivial -- equal inputs have to result in equal outputs.
        // However, it seemed important to test this behavior and I couldn't think of how else to do it.
        [TestMethod()]
        public void ChecksumIdenticalReg()
        {
            List<TimeSeriesValue> duplicateList = new List<TimeSeriesValue>();

            // Create an identical array by deep copy
            foreach (TimeSeriesValue tsv in IrregList1)
            {
                duplicateList.Add(new TimeSeriesValue { Date = tsv.Date, Value = tsv.Value });
            }
            TSTrace trace1 = new TSTrace { TraceNumber = 1 };
            TSTrace trace2 = new TSTrace { TraceNumber = 1 };

            Boolean ret = ComputeTestChecksums(
                    TSDateCalculator.TimeStepUnitCode.Irregular, 0, IrregList1, trace1,
                    TSDateCalculator.TimeStepUnitCode.Irregular, 0, duplicateList, trace2);

            Assert.IsTrue(ret);
        }
        [TestMethod()]
        public void ChecksumDiffTest1()
        {
            List<TimeSeriesValue> duplicateList = new List<TimeSeriesValue>();

            // Create an identical array by deep copy
            foreach (TimeSeriesValue tsv in IrregList1)
            {
                duplicateList.Add(new TimeSeriesValue { Date = tsv.Date, Value = tsv.Value });
            }
            // Slightly alter one date in the list
            duplicateList[5].Date = duplicateList[5].Date.AddMinutes(12);

            TSTrace trace1 = new TSTrace { TraceNumber = 1 };
            TSTrace trace2 = new TSTrace { TraceNumber = 1 };

            Boolean ret = ComputeTestChecksums(
                    TSDateCalculator.TimeStepUnitCode.Irregular, 0, IrregList1, trace1,
                    TSDateCalculator.TimeStepUnitCode.Irregular, 0, duplicateList, trace2);

            Assert.IsFalse(ret);
        }
        [TestMethod()]
        public void ChecksumDiffTest2()
        {
            List<TimeSeriesValue> duplicateList = new List<TimeSeriesValue>();

            // Create an identical array by deep copy
            foreach (TimeSeriesValue tsv in IrregList1)
            {
                duplicateList.Add(new TimeSeriesValue { Date = tsv.Date, Value = tsv.Value });
            }
            // Slightly alter one value in the list
            duplicateList[17].Value *= 1.02;

            TSTrace trace1 = new TSTrace { TraceNumber = 1 };
            TSTrace trace2 = new TSTrace { TraceNumber = 1 };

            Boolean ret = ComputeTestChecksums(
                    TSDateCalculator.TimeStepUnitCode.Irregular, 0, IrregList1, trace1,
                    TSDateCalculator.TimeStepUnitCode.Irregular, 0, duplicateList, trace2);

            Assert.IsFalse(ret);
        }
        [TestMethod()]
        public void ChecksumDiffTest5()
        {
            List<TimeSeriesValue> duplicateList = new List<TimeSeriesValue>();

            // Create an identical array by deep copy
            foreach (TimeSeriesValue tsv in IrregList1)
            {
                duplicateList.Add(new TimeSeriesValue { Date = tsv.Date, Value = tsv.Value });
            }
            // Trace numbers are different
            TSTrace trace1 = new TSTrace { TraceNumber = 1 };
            TSTrace trace2 = new TSTrace { TraceNumber = 3 };

            Boolean ret = ComputeTestChecksums(
                    TSDateCalculator.TimeStepUnitCode.Irregular, 0, IrregList1, trace1,
                    TSDateCalculator.TimeStepUnitCode.Irregular, 0, duplicateList, trace2);

            Assert.IsFalse(ret);
        }
        [TestMethod()]
        public void ChecksumDiffTest2B()
        {
            List<TimeSeriesValue> duplicateList = new List<TimeSeriesValue>();

            // Create an identical array by deep copy
            foreach (TimeSeriesValue tsv in IrregList1)
            {
                duplicateList.Add(new TimeSeriesValue { Date = tsv.Date, Value = tsv.Value });
            }
            // Slightly alter one value in the list
            duplicateList[177].Value *= 1.2;

            TSTrace trace1 = new TSTrace { TraceNumber = 1 };
            TSTrace trace2 = new TSTrace { TraceNumber = 1 };

            Boolean ret = ComputeTestChecksums(
                    TSDateCalculator.TimeStepUnitCode.Irregular, 0, IrregList1, trace1,
                    TSDateCalculator.TimeStepUnitCode.Irregular, 0, duplicateList, trace2);

            Assert.IsFalse(ret);
        }
        [TestMethod()]
        public void ChecksumDiffTest3()
        {
            List<TimeSeriesValue> duplicateList = new List<TimeSeriesValue>();

            // Create an identical array by deep copy
            foreach (TimeSeriesValue tsv in IrregList1)
            {
                duplicateList.Add(new TimeSeriesValue { Date = tsv.Date, Value = tsv.Value });
            }
            // remove the last time step in the list
            duplicateList.RemoveAt(duplicateList.Count - 1);

            TSTrace trace1 = new TSTrace { TraceNumber = 1 };
            TSTrace trace2 = new TSTrace { TraceNumber = 1 };

            Boolean ret = ComputeTestChecksums(
                    TSDateCalculator.TimeStepUnitCode.Irregular, 0, IrregList1, trace1,
                    TSDateCalculator.TimeStepUnitCode.Irregular, 0, duplicateList, trace2);

            Assert.IsFalse(ret);
        }
        [TestMethod()]
        public void ChecksumDiffTest4()
        {
            List<TimeSeriesValue> duplicateList = new List<TimeSeriesValue>();

            // Create an identical array by deep copy
            foreach (TimeSeriesValue tsv in IrregList1)
            {
                duplicateList.Add(new TimeSeriesValue { Date = tsv.Date, Value = tsv.Value });
            }
            // remove a value from the middle of the list
            duplicateList.RemoveAt(150);

            TSTrace trace1 = new TSTrace { TraceNumber = 1 };
            TSTrace trace2 = new TSTrace { TraceNumber = 1 };

            Boolean ret = ComputeTestChecksums(
                    TSDateCalculator.TimeStepUnitCode.Irregular, 0, IrregList1, trace1,
                    TSDateCalculator.TimeStepUnitCode.Irregular, 0, duplicateList, trace2);

            Assert.IsFalse(ret);
        }
        #endregion


        #region Test Methods for error handling of ConvertListToBlobWithChecksum()

        // Method should not throw any exceptions.
        [TestMethod()]
        public void ConvertListToBlobWithChecksum_Err0()
        {
            TSLibrary tsLib = new TSLibrary();
            TSTrace traceObject = new TSTrace { TraceNumber = 1 };
            int compressionCode;

            try
            {
                byte[] blobData = tsLib.ConvertListToBlobWithChecksum(
                        TSDateCalculator.TimeStepUnitCode.Irregular, 0,
                        IrregList1.Count, IrregList1.First().Date, IrregList1.Last().Date,
                        IrregList1, traceObject, out compressionCode);
                Assert.IsTrue(true);
            }
            catch (TSLibraryException)
            {
                Assert.Fail("Should not throw any exceptions");
            }
        }
        // Method should throw an exception b/c the TimeStepCount is wrong
        [TestMethod()]
        public void ConvertListToBlobWithChecksum_Err2()
        {
            TSLibrary tsLib = new TSLibrary();
            TSTrace traceObject = new TSTrace { TraceNumber = 1 };
            int compressionCode;

            try
            {
                byte[] blobData = tsLib.ConvertListToBlobWithChecksum(
                        TSDateCalculator.TimeStepUnitCode.Irregular, 0,
                        IrregList1.Count+3, IrregList1.First().Date, IrregList1.Last().Date,
                        IrregList1, traceObject, out compressionCode);
                Assert.Fail("Should have thrown exception");
            }
            catch (TSLibraryException e)
            {
                Assert.AreEqual(ErrCode.Enum.Checksum_Improper_Count, e.ErrCode);
            }
        }
        // Method should throw an exception b/c the StartDate is wrong
        [TestMethod()]
        public void ConvertListToBlobWithChecksum_Err3()
        {
            TSLibrary tsLib = new TSLibrary();
            TSTrace traceObject = new TSTrace { TraceNumber = 1 };
            int compressionCode;

            try
            {
                byte[] blobData = tsLib.ConvertListToBlobWithChecksum(
                        TSDateCalculator.TimeStepUnitCode.Irregular, 0,
                        IrregList1.Count, IrregList1.First().Date.AddDays(2), IrregList1.Last().Date,
                        IrregList1, traceObject, out compressionCode);
                Assert.Fail("Should have thrown exception");
            }
            catch (TSLibraryException e)
            {
                Assert.AreEqual(ErrCode.Enum.Checksum_Improper_StartDate, e.ErrCode);
            }
        }
        // Method should throw an exception b/c the end date is wrong
        [TestMethod()]
        public void ConvertListToBlobWithChecksum_Err4()
        {
            TSLibrary tsLib = new TSLibrary();
            TSTrace traceObject = new TSTrace { TraceNumber = 1 };
            int compressionCode;

            try
            {
                byte[] blobData = tsLib.ConvertListToBlobWithChecksum(
                        TSDateCalculator.TimeStepUnitCode.Irregular, 0,
                        IrregList1.Count, IrregList1.First().Date, IrregList1.Last().Date.AddDays(2),
                        IrregList1, traceObject, out compressionCode);
                Assert.Fail("Should have thrown exception");
            }
            catch (TSLibraryException e)
            {
                Assert.AreEqual(ErrCode.Enum.Checksum_Improper_EndDate, e.ErrCode);
            }
        }

        #endregion


        #region Test Methods for ComputeChecksum()

        #region helper methods
        // This method is reused by the actual test methods that follow
        public Boolean ComputeTestChecksums(
                TSDateCalculator.TimeStepUnitCode u1, short q1,
                    DateTime sDate1, List<ITimeSeriesTrace> traceList1,
                TSDateCalculator.TimeStepUnitCode u2, short q2,
                    DateTime sDate2, List<ITimeSeriesTrace> traceList2)
        {
            TSLibrary tsLib = new TSLibrary();

            byte[] b1 = tsLib.ComputeChecksum(u1, q1, sDate1, traceList1);
            byte[] b2 = tsLib.ComputeChecksum(u2, q2, sDate2, traceList2);

            Assert.IsTrue(b1.Length == 16);
            Assert.IsTrue(b2.Length == 16);

            for (int i = 0; i < b2.Length; i++)
                if (b1[i] != b2[i])
                    return false;

            return true;
        }
        public List<ITimeSeriesTrace> Get3TracesRegular(
                    TSDateCalculator.TimeStepUnitCode u1, short q1, int c1,
                    DateTime sDate1, out DateTime eDate1,
                    Boolean shouldPerturb = false,
                    int perturbTraceIndex = 0, int perturbStep = 0, 
                    double perturbVal = 0, double perturbMinutes = 0)
        {
            eDate1 = DateTime.Now;  // dummy
            TSLibrary tsLib = new TSLibrary();
            List<ITimeSeriesTrace> traceList = new List<ITimeSeriesTrace>();
            int compressionCode;

            for (int t = 0; t < 3; t++)
            {
                TSTrace trace1 = new TSTrace { TraceNumber = t + 1 };
                List<TimeSeriesValue> tsValues = new List<TimeSeriesValue>();
                DateTime curDate = sDate1;
                Double curVal = 20;
                for (int i = 0; i < c1; i++)
                {
                    tsValues.Add(new TimeSeriesValue { Date = curDate, Value = curVal });
                    curDate = tsLib.IncrementDate(curDate, u1, q1, 1);
                    curVal = curVal + i / (t + 1);
                }
                // make a perturbation if called for
                if (shouldPerturb && t==perturbTraceIndex)
                {
                    tsValues[perturbStep].Value += perturbVal;
                    tsValues[perturbStep].Date = tsValues[perturbStep].Date.AddMinutes(perturbMinutes);
                }
                eDate1 = tsValues.Last().Date;
                tsLib.ConvertListToBlobWithChecksum(
                        u1, q1, c1,
                        sDate1, eDate1, tsValues, trace1, out compressionCode);

                traceList.Add(trace1);
            }
            return traceList;
        }
        public List<ITimeSeriesTrace> Get3TracesIrregular(
                    int c1, DateTime sDate1, out DateTime eDate1,
                    Boolean shouldPerturb = false,
                    int perturbTraceIndex = 0, int perturbStep = 0,
                    double perturbVal = 0, double perturbMinutes = 0)
        {
            eDate1 = DateTime.Now;  // dummy
            TSLibrary tsLib = new TSLibrary();
            List<ITimeSeriesTrace> traceList = new List<ITimeSeriesTrace>();
            int compressionCode;

            for (int t = 0; t < 3; t++)
            {
                TSTrace trace1 = new TSTrace { TraceNumber = t + 1 };
                List<TimeSeriesValue> tsValues = new List<TimeSeriesValue>();
                DateTime curDate = sDate1;
                Double curVal = 20;
                for (int i = 0; i < c1; i++)
                {
                    tsValues.Add(new TimeSeriesValue { Date = curDate, Value = curVal });
                    curDate = curDate.AddDays(i / 2);
                    curVal = curVal + i / (t+1);
                }
                // make a perturbation if called for
                if (shouldPerturb && t == perturbTraceIndex)
                {
                    tsValues[perturbStep].Value += perturbVal;
                    tsValues[perturbStep].Date = tsValues[perturbStep].Date.AddMinutes(perturbMinutes);
                }
                eDate1 = tsValues.Last().Date;
                tsLib.ConvertListToBlobWithChecksum(
                        TSDateCalculator.TimeStepUnitCode.Irregular, 0, c1,
                        sDate1, eDate1, tsValues, trace1, out compressionCode);

                traceList.Add(trace1);
            }
            return traceList;
        } 
        #endregion

        #region test regular time step
        [TestMethod()]
        public void ComputeChecksumIdenticalReg()
        {
            TSDateCalculator.TimeStepUnitCode timeStepUnit = TSDateCalculator.TimeStepUnitCode.Day;
            short timeStepQuanitity = 2;
            int timeStepCount = 50;
            DateTime sDate = DateTime.Parse("1/1/1935"), eDate;

            List<ITimeSeriesTrace> traceList1 = Get3TracesRegular
                    (timeStepUnit, timeStepQuanitity, timeStepCount, sDate, out eDate);
            List<ITimeSeriesTrace> traceList2 = Get3TracesRegular
                    (timeStepUnit, timeStepQuanitity, timeStepCount, sDate, out eDate);

            Boolean ret = ComputeTestChecksums(
                    timeStepUnit, timeStepQuanitity, sDate, traceList1,
                    timeStepUnit, timeStepQuanitity, sDate, traceList2);

            Assert.IsTrue(ret);
        }
        [TestMethod()]
        public void ComputeChecksumDiffReg1()
        {
            TSDateCalculator.TimeStepUnitCode timeStepUnit = TSDateCalculator.TimeStepUnitCode.Day;
            short timeStepQuanitity = 2;
            int timeStepCount = 50;
            DateTime sDate = DateTime.Parse("1/1/1935"), eDate;

            List<ITimeSeriesTrace> traceList1 = Get3TracesRegular
                    (timeStepUnit, timeStepQuanitity, timeStepCount, sDate, out eDate);
            List<ITimeSeriesTrace> traceList2 = Get3TracesRegular
                    (timeStepUnit, timeStepQuanitity, timeStepCount, sDate, out eDate);

            // remove one trace from list 1
            traceList1.RemoveAt(2);

            Boolean ret = ComputeTestChecksums(
                    timeStepUnit, timeStepQuanitity, sDate, traceList1,
                    timeStepUnit, timeStepQuanitity, sDate, traceList2);

            Assert.IsFalse(ret);
        }
        [TestMethod()]
        public void ComputeChecksumDiffReg2()
        {
            TSDateCalculator.TimeStepUnitCode timeStepUnit = TSDateCalculator.TimeStepUnitCode.Day;
            short timeStepQuanitity = 2;
            int timeStepCount = 50;
            DateTime sDate = DateTime.Parse("1/1/1935"), eDate;

            List<ITimeSeriesTrace> traceList1 = Get3TracesRegular
                    (timeStepUnit, timeStepQuanitity, timeStepCount, sDate, out eDate,
                    true, 1, 20, 13.2);  // change one value
            List<ITimeSeriesTrace> traceList2 = Get3TracesRegular
                    (timeStepUnit, timeStepQuanitity, timeStepCount, sDate, out eDate);

            Boolean ret = ComputeTestChecksums(
                    timeStepUnit, timeStepQuanitity, sDate, traceList1,
                    timeStepUnit, timeStepQuanitity, sDate, traceList2);

            Assert.IsFalse(ret);
        }
        [TestMethod()]
        public void ComputeChecksumDiffReg3()
        {
            TSDateCalculator.TimeStepUnitCode timeStepUnit = TSDateCalculator.TimeStepUnitCode.Day;
            short timeStepQuanitity = 2;
            int timeStepCount = 50;
            DateTime sDate = DateTime.Parse("1/1/1935"), eDate;

            List<ITimeSeriesTrace> traceList1 = Get3TracesRegular
                    (timeStepUnit, timeStepQuanitity, timeStepCount, sDate, out eDate,
                    true, 2, 40, 0.0001);  // change one value
            List<ITimeSeriesTrace> traceList2 = Get3TracesRegular
                    (timeStepUnit, timeStepQuanitity, timeStepCount, sDate, out eDate);

            Boolean ret = ComputeTestChecksums(
                    timeStepUnit, timeStepQuanitity, sDate, traceList1,
                    timeStepUnit, timeStepQuanitity, sDate, traceList2);

            Assert.IsFalse(ret);
        }
        [TestMethod()]
        public void ComputeChecksumIdenticalRegA()
        {
            TSDateCalculator.TimeStepUnitCode timeStepUnit = TSDateCalculator.TimeStepUnitCode.Day;
            short timeStepQuanitity = 2;
            int timeStepCount = 50;
            DateTime sDate = DateTime.Parse("1/1/1935"), eDate;

            // Only use one trace
            List<ITimeSeriesTrace> traceList1 = Get3TracesRegular
                    (timeStepUnit, timeStepQuanitity, timeStepCount, sDate, out eDate).Take(1).ToList();
            List<ITimeSeriesTrace> traceList2 = Get3TracesRegular
                    (timeStepUnit, timeStepQuanitity, timeStepCount, sDate, out eDate).Take(1).ToList();

            Boolean ret = ComputeTestChecksums(
                    timeStepUnit, timeStepQuanitity, sDate, traceList1,
                    timeStepUnit, timeStepQuanitity, sDate, traceList2);

            Assert.IsTrue(ret);
        }
        [TestMethod()]
        public void ComputeChecksumDiffRegA1()
        {
            TSDateCalculator.TimeStepUnitCode timeStepUnit = TSDateCalculator.TimeStepUnitCode.Day;
            short timeStepQuanitity = 2;
            int timeStepCount = 50;
            DateTime sDate = DateTime.Parse("1/1/1935"), eDate;

            // Only use one trace
            List<ITimeSeriesTrace> traceList1 = Get3TracesRegular
                    (timeStepUnit, timeStepQuanitity, timeStepCount, sDate, out eDate,
                    true, 0, 2, 0.2).Take(1).ToList(); // change one value
            List<ITimeSeriesTrace> traceList2 = Get3TracesRegular
                    (timeStepUnit, timeStepQuanitity, timeStepCount, sDate, out eDate).Take(1).ToList();

            Boolean ret = ComputeTestChecksums(
                    timeStepUnit, timeStepQuanitity, sDate, traceList1,
                    timeStepUnit, timeStepQuanitity, sDate, traceList2);

            Assert.IsFalse(ret);
        }
        [TestMethod()]
        public void ComputeChecksumDiffRegParam1()
        {
            TSDateCalculator.TimeStepUnitCode timeStepUnit1 = TSDateCalculator.TimeStepUnitCode.Day;
            short timeStepQuanitity1 = 2;
            int timeStepCount1 = 50;
            DateTime sDate1 = DateTime.Parse("1/1/1935"), eDate1;

            TSDateCalculator.TimeStepUnitCode timeStepUnit2 = TSDateCalculator.TimeStepUnitCode.Hour; // different
            short timeStepQuanitity2 = 2;
            int timeStepCount2 = 50;
            DateTime sDate2 = DateTime.Parse("1/1/1935"), eDate2;

            List<ITimeSeriesTrace> traceList1 = Get3TracesRegular
                    (timeStepUnit1, timeStepQuanitity1, timeStepCount1, sDate1, out eDate1);
            List<ITimeSeriesTrace> traceList2 = Get3TracesRegular
                    (timeStepUnit2, timeStepQuanitity2, timeStepCount2, sDate2, out eDate2);

            Boolean ret = ComputeTestChecksums(
                    timeStepUnit1, timeStepQuanitity1, sDate1, traceList1,
                    timeStepUnit2, timeStepQuanitity2, sDate2, traceList2);

            Assert.IsFalse(ret);
        }
        [TestMethod()]
        public void ComputeChecksumDiffRegParam2()
        {
            TSDateCalculator.TimeStepUnitCode timeStepUnit1 = TSDateCalculator.TimeStepUnitCode.Day;
            short timeStepQuanitity1 = 2;
            int timeStepCount1 = 50;
            DateTime sDate1 = DateTime.Parse("1/1/1935"), eDate1;

            TSDateCalculator.TimeStepUnitCode timeStepUnit2 = TSDateCalculator.TimeStepUnitCode.Day;
            short timeStepQuanitity2 = 1; // different
            int timeStepCount2 = 50;
            DateTime sDate2 = DateTime.Parse("1/1/1935"), eDate2;

            List<ITimeSeriesTrace> traceList1 = Get3TracesRegular
                    (timeStepUnit1, timeStepQuanitity1, timeStepCount1, sDate1, out eDate1);
            List<ITimeSeriesTrace> traceList2 = Get3TracesRegular
                    (timeStepUnit2, timeStepQuanitity2, timeStepCount2, sDate2, out eDate2);

            Boolean ret = ComputeTestChecksums(
                    timeStepUnit1, timeStepQuanitity1, sDate1, traceList1,
                    timeStepUnit2, timeStepQuanitity2, sDate2, traceList2);

            Assert.IsFalse(ret);
        }
        [TestMethod()]
        public void ComputeChecksumDiffRegParam4()
        {
            TSDateCalculator.TimeStepUnitCode timeStepUnit1 = TSDateCalculator.TimeStepUnitCode.Day;
            short timeStepQuanitity1 = 2;
            int timeStepCount1 = 50;
            DateTime sDate1 = DateTime.Parse("1/1/1935"), eDate1;

            TSDateCalculator.TimeStepUnitCode timeStepUnit2 = TSDateCalculator.TimeStepUnitCode.Day;
            short timeStepQuanitity2 = 2;
            int timeStepCount2 = 50;
            DateTime sDate2 = DateTime.Parse("1/2/1935"), eDate2; // different

            List<ITimeSeriesTrace> traceList1 = Get3TracesRegular
                    (timeStepUnit1, timeStepQuanitity1, timeStepCount1, sDate1, out eDate1);
            List<ITimeSeriesTrace> traceList2 = Get3TracesRegular
                    (timeStepUnit2, timeStepQuanitity2, timeStepCount2, sDate2, out eDate2);

            Boolean ret = ComputeTestChecksums(
                    timeStepUnit1, timeStepQuanitity1, sDate1, traceList1,
                    timeStepUnit2, timeStepQuanitity2, sDate2, traceList2);

            Assert.IsFalse(ret);
        } 
        #endregion

        #region test irregular time step
        [TestMethod()]
        public void ComputeChecksumIdenticalIrreg()
        {
            TSDateCalculator.TimeStepUnitCode timeStepUnit = TSDateCalculator.TimeStepUnitCode.Day;
            short timeStepQuanitity = 2;
            int timeStepCount = 50;
            DateTime sDate = DateTime.Parse("1/1/1935"), eDate;

            List<ITimeSeriesTrace> traceList1 = Get3TracesIrregular
                    (timeStepCount, sDate, out eDate);
            List<ITimeSeriesTrace> traceList2 = Get3TracesIrregular
                    (timeStepCount, sDate, out eDate);

            Boolean ret = ComputeTestChecksums(
                    timeStepUnit, timeStepQuanitity, sDate, traceList1,
                    timeStepUnit, timeStepQuanitity, sDate, traceList2);

            Assert.IsTrue(ret);
        }
        [TestMethod()]
        public void ComputeChecksumDiffIrreg1()
        {
            TSDateCalculator.TimeStepUnitCode timeStepUnit = TSDateCalculator.TimeStepUnitCode.Day;
            short timeStepQuanitity = 2;
            int timeStepCount = 50;
            DateTime sDate = DateTime.Parse("1/1/1935"), eDate;

            List<ITimeSeriesTrace> traceList1 = Get3TracesIrregular
                    (timeStepCount, sDate, out eDate);
            List<ITimeSeriesTrace> traceList2 = Get3TracesIrregular
                    (timeStepCount, sDate, out eDate);

            // remove one trace from list 1
            traceList1.RemoveAt(2);

            Boolean ret = ComputeTestChecksums(
                    timeStepUnit, timeStepQuanitity, sDate, traceList1,
                    timeStepUnit, timeStepQuanitity, sDate, traceList2);

            Assert.IsFalse(ret);
        }
        [TestMethod()]
        public void ComputeChecksumDiffIrreg2()
        {
            TSDateCalculator.TimeStepUnitCode timeStepUnit = TSDateCalculator.TimeStepUnitCode.Day;
            short timeStepQuanitity = 2;
            int timeStepCount = 50;
            DateTime sDate = DateTime.Parse("1/1/1935"), eDate;

            List<ITimeSeriesTrace> traceList1 = Get3TracesIrregular
                    (timeStepCount, sDate, out eDate,
                    true, 1, 20, 13.2);  // change one value
            List<ITimeSeriesTrace> traceList2 = Get3TracesIrregular
                    (timeStepCount, sDate, out eDate);

            Boolean ret = ComputeTestChecksums(
                    timeStepUnit, timeStepQuanitity, sDate, traceList1,
                    timeStepUnit, timeStepQuanitity, sDate, traceList2);

            Assert.IsFalse(ret);
        }
        [TestMethod()]
        public void ComputeChecksumDiffIrreg3()
        {
            TSDateCalculator.TimeStepUnitCode timeStepUnit = TSDateCalculator.TimeStepUnitCode.Day;
            short timeStepQuanitity = 2;
            int timeStepCount = 50;
            DateTime sDate = DateTime.Parse("1/1/1935"), eDate;

            List<ITimeSeriesTrace> traceList1 = Get3TracesIrregular
                    (timeStepCount, sDate, out eDate,
                    true, 2, 40, 0.0001);  // change one value
            List<ITimeSeriesTrace> traceList2 = Get3TracesIrregular
                    (timeStepCount, sDate, out eDate);

            Boolean ret = ComputeTestChecksums(
                    timeStepUnit, timeStepQuanitity, sDate, traceList1,
                    timeStepUnit, timeStepQuanitity, sDate, traceList2);

            Assert.IsFalse(ret);
        }
        [TestMethod()]
        public void ComputeChecksumIdenticalIrregA()
        {
            TSDateCalculator.TimeStepUnitCode timeStepUnit = TSDateCalculator.TimeStepUnitCode.Day;
            short timeStepQuanitity = 2;
            int timeStepCount = 50;
            DateTime sDate = DateTime.Parse("1/1/1935"), eDate;

            // Only use one trace
            List<ITimeSeriesTrace> traceList1 = Get3TracesIrregular
                    (timeStepCount, sDate, out eDate).Take(1).ToList();
            List<ITimeSeriesTrace> traceList2 = Get3TracesIrregular
                    (timeStepCount, sDate, out eDate).Take(1).ToList();

            Boolean ret = ComputeTestChecksums(
                    timeStepUnit, timeStepQuanitity, sDate, traceList1,
                    timeStepUnit, timeStepQuanitity, sDate, traceList2);

            Assert.IsTrue(ret);
        }
        [TestMethod()]
        public void ComputeChecksumDiffIrregA1()
        {
            TSDateCalculator.TimeStepUnitCode timeStepUnit = TSDateCalculator.TimeStepUnitCode.Day;
            short timeStepQuanitity = 2;
            int timeStepCount = 50;
            DateTime sDate = DateTime.Parse("1/1/1935"), eDate;

            // Only use one trace
            List<ITimeSeriesTrace> traceList1 = Get3TracesIrregular
                    (timeStepCount, sDate, out eDate,
                    true, 0, 2, 0.2).Take(1).ToList(); // change one value
            List<ITimeSeriesTrace> traceList2 = Get3TracesIrregular
                    (timeStepCount, sDate, out eDate).Take(1).ToList();

            Boolean ret = ComputeTestChecksums(
                    timeStepUnit, timeStepQuanitity, sDate, traceList1,
                    timeStepUnit, timeStepQuanitity, sDate, traceList2);

            Assert.IsFalse(ret);
        }
        [TestMethod()]
        public void ComputeChecksumDiffIrregParam4()
        {
            TSDateCalculator.TimeStepUnitCode timeStepUnit1 = TSDateCalculator.TimeStepUnitCode.Day;
            short timeStepQuanitity1 = 2;
            int timeStepCount1 = 50;
            DateTime sDate1 = DateTime.Parse("1/1/1935"), eDate1;

            TSDateCalculator.TimeStepUnitCode timeStepUnit2 = TSDateCalculator.TimeStepUnitCode.Day;
            short timeStepQuanitity2 = 2;
            int timeStepCount2 = 50;
            DateTime sDate2 = DateTime.Parse("1/2/1935"), eDate2; // different

            List<ITimeSeriesTrace> traceList1 = Get3TracesIrregular
                    (timeStepCount1, sDate1, out eDate1);
            List<ITimeSeriesTrace> traceList2 = Get3TracesIrregular
                    (timeStepCount2, sDate2, out eDate2);

            Boolean ret = ComputeTestChecksums(
                    timeStepUnit1, timeStepQuanitity1, sDate1, traceList1,
                    timeStepUnit2, timeStepQuanitity2, sDate2, traceList2);

            Assert.IsFalse(ret);
        }
        #endregion



        // Method should throw an exception b/c the TimeStepQuantity must
        // be zero when the TimeStepUnit==Irregular.
        [TestMethod()]
        public void ComputeChecksum_Err1()
        {
            TSLibrary tsLib = new TSLibrary();
            TSTrace traceObject = new TSTrace { TraceNumber = 1 };

            try
            {
                byte[] blobData = tsLib.ComputeChecksum(
                        TSDateCalculator.TimeStepUnitCode.Irregular, 3,
                        IrregList1.First().Date, new List<ITimeSeriesTrace>());
                Assert.Fail("Should have thrown exception");
            }
            catch (TSLibraryException e)
            {
                Assert.AreEqual(ErrCode.Enum.Checksum_Quantity_Nonzero, e.ErrCode);
            }
        }

        #endregion

    }
}
