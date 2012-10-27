using TimeSeriesLibrary;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;

namespace TimeSeriesLibrary_Test
{
    
    
    /// <summary>
    ///This is a test class for TSXmlTest and is intended
    ///to contain all TSXmlTest Unit Tests
    ///</summary>
    [TestClass()]
    public class TSXmlTest
    {
        TSXml TsXml;
        List<TSImport> TsImportList;
        TSLibrary TsLib = new TSLibrary();

        #region TestContext
        private TestContext testContextInstance;
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
        //
        //Use ClassInitialize to run code before running the first test in the class
        //[ClassInitialize()]
        //public static void MyClassInitialize(TestContext testContext)
        //{
        //}
        //
        //Use ClassCleanup to run code after all tests in a class have run
        //[ClassCleanup()]
        //public static void MyClassCleanup()
        //{
        //}
        
        //Use TestInitialize to run code before running each test
        [TestInitialize()]
        public void MyTestInitialize()
        {
            TsXml = new TSXml();
            TsImportList = new List<TSImport>();
        }
        
        //Use TestCleanup to run code after each test has run
        [TestCleanup()]
        public void MyTestCleanup()
        {
            TsXml = null;
            TsImportList = null;
        }
        
        #endregion


        #region Test Method for ReadAndStore() single-trace without errors
        [TestMethod()]
        public void ReadAndStoreSingleTrace()
        {
            string xmlText = Properties.Resources.test1;
            bool storeToDatabase = false;
            bool recordDetails = true;

            int ret = TsXml.ReadAndStore(null, xmlText, TsImportList, storeToDatabase, recordDetails);

            // Correct return value from method?
            Assert.AreEqual(ret, 3);
            // Correct number of items added to import list?
            Assert.AreEqual(TsImportList.Count, 3);

            // Verify IsDetailed property
            Assert.AreEqual(TsImportList[1].IsDetailed, true);

            // Verify names in import list
            Assert.AreEqual(TsImportList[0].Name, "JOEBLOW");
            Assert.AreEqual(TsImportList[1].Name, "BILLBLOW");
            Assert.AreEqual(TsImportList[2].Name, "IRREGULARCLARENCE");

            // Verify TimeStepUnit in import list
            Assert.AreEqual(TsImportList[0].TimeStepUnit, TSDateCalculator.TimeStepUnitCode.Hour);
            Assert.AreEqual(TsImportList[1].TimeStepUnit, TSDateCalculator.TimeStepUnitCode.Day);
            Assert.AreEqual(TsImportList[2].TimeStepUnit, TSDateCalculator.TimeStepUnitCode.Irregular);

            // Verify TimeStepQuantity in import list
            Assert.AreEqual(TsImportList[0].TimeStepQuantity, 1);
            Assert.AreEqual(TsImportList[1].TimeStepQuantity, 2);
            // A dummy nonzero value was put into the XML file, but the library must ensure that
            // the output is zero in order to ensure consistency in the checksums.
            Assert.AreEqual(TsImportList[2].TimeStepQuantity, 0);

            // Verify TimeStepCount in import list
            Assert.AreEqual(TsImportList[0].TimeStepCount, 9);
            Assert.AreEqual(TsImportList[1].TimeStepCount, 11);
            Assert.AreEqual(TsImportList[2].TimeStepCount, 17);

            // Verify BlobStartDate in import list
            Assert.AreEqual(TsImportList[0].BlobStartDate, DateTime.Parse("10/01/1927 03:09:00"));
            Assert.AreEqual(TsImportList[1].BlobStartDate, DateTime.Parse("10/01/1927 23:59:00"));
            Assert.AreEqual(TsImportList[2].BlobStartDate, DateTime.Parse("1/01/1930 11:59:00"));

            // Verify BlobEndDate in import list
            Assert.AreEqual(TsImportList[0].BlobEndDate, DateTime.Parse("10/01/1927 11:09:00"));
            Assert.AreEqual(TsImportList[1].BlobEndDate, DateTime.Parse("10/21/1927 23:59:00"));
            Assert.AreEqual(TsImportList[2].BlobEndDate, DateTime.Parse("01/15/1930 23:59:00"));

            // Verify trace count in import list
            Assert.AreEqual(TsImportList[0].TraceList.Count, 1);
            Assert.AreEqual(TsImportList[1].TraceList.Count, 1);
            Assert.AreEqual(TsImportList[2].TraceList.Count, 1);

            // Verify Checksum in import list
            // The checksums are verified against previous values, so the test is only as good as the
            // original trial.  However, it is good if the test flags any change, to ensure that the
            // developer can account for any change.  E.g., if the original value was incorrect, then
            // that should be documented and the new output should be scrutinized.
            Assert.AreEqual(BitConverter.ToString(TsImportList[0].Checksum), "AA-EE-42-CD-C9-80-0D-28-DB-3B-A9-0B-81-1D-E8-E9");
            Assert.AreEqual(BitConverter.ToString(TsImportList[1].Checksum), "BE-FB-A0-80-94-E4-27-55-43-0D-5C-1E-BA-CC-9E-5E");
            Assert.AreEqual(BitConverter.ToString(TsImportList[2].Checksum), "52-75-33-AB-4D-2D-5B-D5-44-6F-2D-55-55-A2-76-1B");

            List<TimeSeriesValue> tsvList = null;
            // Verify BLOB # 1 in import list
            TsLib.ConvertBlobToListAll(TsImportList[0].TimeStepUnit, TsImportList[0].TimeStepQuantity,
                        TsImportList[0].BlobStartDate, TsImportList[0].TraceList[0].ValueBlob,
                        ref tsvList);
            Assert.AreEqual(tsvList.Count, 9);
            Assert.AreEqual(tsvList[0].Value, 12.3);
            Assert.AreEqual(tsvList[1].Value, 21.5);
            Assert.AreEqual(tsvList[7].Value, 20.1);
            Assert.AreEqual(tsvList[8].Value, 12.4);
            // Verify BLOB # 2 in import list
            TsLib.ConvertBlobToListAll(TsImportList[1].TimeStepUnit, TsImportList[1].TimeStepQuantity,
                        TsImportList[1].BlobStartDate, TsImportList[1].TraceList[0].ValueBlob,
                        ref tsvList);
            Assert.AreEqual(tsvList.Count, 11);
            Assert.AreEqual(tsvList[0].Value, 12.5);
            Assert.AreEqual(tsvList[1].Value, 21.7);
            Assert.AreEqual(tsvList[9].Value, 12.2);
            Assert.AreEqual(tsvList[10].Value, 12.6);
            // Verify BLOB # 3 in import list
            TsLib.ConvertBlobToListAll(TsImportList[2].TimeStepUnit, TsImportList[2].TimeStepQuantity,
                        TsImportList[2].BlobStartDate, TsImportList[2].TraceList[0].ValueBlob,
                        ref tsvList);
            Assert.AreEqual(tsvList.Count, 17);
            Assert.AreEqual(tsvList[0].Value, 312.5); Assert.AreEqual(tsvList[0].Date, DateTime.Parse("01/01/1930 11:59:00"));
            Assert.AreEqual(tsvList[5].Value, 360.8); Assert.AreEqual(tsvList[5].Date, DateTime.Parse("01/03/1930 23:59:00"));
            Assert.AreEqual(tsvList[14].Value, 2312.2); Assert.AreEqual(tsvList[14].Date, DateTime.Parse("01/11/1930 23:59:00"));
            Assert.AreEqual(tsvList[16].Value, 312.2); Assert.AreEqual(tsvList[16].Date, DateTime.Parse("01/15/1930 23:59:00"));



            // Verify APart in import list
            Assert.AreEqual(TsImportList[0].APart, null);
            Assert.AreEqual(TsImportList[1].APart, "QQ");
            // Verify BPart in import list
            Assert.AreEqual(TsImportList[0].BPart, null);
            Assert.AreEqual(TsImportList[1].BPart, "RR");
            // Verify CPart in import list
            Assert.AreEqual(TsImportList[0].CPart, null);
            Assert.AreEqual(TsImportList[1].CPart, "SS");
            // Verify EPart in import list
            Assert.AreEqual(TsImportList[0].EPart, null);
            Assert.AreEqual(TsImportList[1].EPart, "TT");
            // Verify Units in import list
            Assert.AreEqual(TsImportList[0].Units, "CFS");
            Assert.AreEqual(TsImportList[1].Units, "CFS");
            Assert.AreEqual(TsImportList[2].Units, "AFD");
            // Verify TimeSeriesType in import list
            Assert.AreEqual(TsImportList[0].TimeSeriesType, "PER-CUM");
            Assert.AreEqual(TsImportList[1].TimeSeriesType, "INST-VAL");
            Assert.AreEqual(TsImportList[2].TimeSeriesType, "INST-VAL");
            // Verify TraceNumber in import list
            Assert.AreEqual(TsImportList[0].TraceList[0].TraceNumber, 2);
            Assert.AreEqual(TsImportList[1].TraceList[0].TraceNumber, 5);
            Assert.AreEqual(TsImportList[2].TraceList[0].TraceNumber, 22);
            // Verify UnprocessedElements in import list
            Assert.AreEqual(TsImportList[1].UnprocessedElements, null);
            Assert.AreEqual(TsImportList[2].UnprocessedElements, " <OldManTag></OldManTag> <Schlap>Spoing-oing-oing</Schlap>");

        } 
        #endregion

        #region Test Method for ReadAndStore() multi-trace without errors
        // Test method for newer multi-trace import
        [TestMethod()]
        public void ReadAndStoreMultiTrace()
        {
            string xmlText = Properties.Resources.test2;
            bool storeToDatabase = false;
            bool recordDetails = true;

            int ret = TsXml.ReadAndStore(null, xmlText, TsImportList, storeToDatabase, recordDetails);

            // Correct return value from method?
            Assert.AreEqual(ret, 2);
            // Correct number of items added to import list?
            Assert.AreEqual(TsImportList.Count, 2);

            // Verify IsDetailed property
            Assert.AreEqual(TsImportList[1].IsDetailed, true);

            // Verify names in import list
            Assert.AreEqual(TsImportList[0].Name, "ManyTrace");
            Assert.AreEqual(TsImportList[1].Name, "ManyUnregulated~Trace\nnewline baby%good");

            // Verify TimeStepUnit in import list
            Assert.AreEqual(TsImportList[0].TimeStepUnit, TSDateCalculator.TimeStepUnitCode.Hour);
            Assert.AreEqual(TsImportList[1].TimeStepUnit, TSDateCalculator.TimeStepUnitCode.Irregular);

            // Verify TimeStepQuantity in import list
            Assert.AreEqual(TsImportList[0].TimeStepQuantity, 1);
            // A dummy nonzero value was put into the XML file, but the library must ensure that
            // the output is zero in order to ensure consistency in the checksums.
            Assert.AreEqual(TsImportList[1].TimeStepQuantity, 0);

            // Verify TimeStepCount in import list
            Assert.AreEqual(TsImportList[0].TimeStepCount, 10);
            Assert.AreEqual(TsImportList[1].TimeStepCount, 17);

            // Verify BlobStartDate in import list
            Assert.AreEqual(TsImportList[0].BlobStartDate, DateTime.Parse("10/01/1927 03:09:00"));
            Assert.AreEqual(TsImportList[1].BlobStartDate, DateTime.Parse("1/01/1930 11:59:00"));

            // Verify BlobEndDate in import list
            Assert.AreEqual(TsImportList[0].BlobEndDate, DateTime.Parse("10/01/1927 12:09:00"));
            Assert.AreEqual(TsImportList[1].BlobEndDate, DateTime.Parse("01/15/1930 23:59:00"));

            // Verify trace count in import list
            Assert.AreEqual(TsImportList[0].TraceList.Count, 3);
            Assert.AreEqual(TsImportList[1].TraceList.Count, 4);

            // Verify Checksum in import list
            // The checksums are verified against previous values, so the test is only as good as the
            // original trial.  However, it is good if the test flags any change, to ensure that the
            // developer can account for any change.  E.g., if the original value was incorrect, then
            // that should be documented and the new output should be scrutinized.
            Assert.AreEqual(BitConverter.ToString(TsImportList[0].Checksum),	"13-68-DE-5F-43-88-F5-D2-C9-75-E6-6F-88-92-3F-AB");
            Assert.AreEqual(BitConverter.ToString(TsImportList[1].Checksum),	"7A-C6-58-6F-27-C0-D1-6F-FB-A3-43-1A-5F-AE-34-3A");
            // Verify Checksum in individual traces
            Assert.AreEqual(BitConverter.ToString(TsImportList[0].TraceList.Single(t => t.TraceNumber == 1).Checksum), "40-1B-D0-A2-72-96-03-15-A7-C5-42-E2-F6-CE-EA-DC");
            Assert.AreEqual(BitConverter.ToString(TsImportList[0].TraceList.Single(t => t.TraceNumber == 2).Checksum), "71-59-86-01-74-86-05-52-A0-AB-34-45-C4-C3-26-78");
            Assert.AreEqual(BitConverter.ToString(TsImportList[1].TraceList.Single(t => t.TraceNumber == 2).Checksum), "B4-9D-01-6E-61-48-02-F3-6C-E6-8C-0F-59-70-5D-7A");
            Assert.AreEqual(BitConverter.ToString(TsImportList[1].TraceList.Single(t => t.TraceNumber == 3).Checksum), "48-32-9C-FA-9A-DC-7D-83-AF-C6-AF-3E-60-A8-A4-EB");

            List<TimeSeriesValue> tsvList = null;
            // Verify BLOB of Series# 1 Trace# 1 in import list
            TsLib.ConvertBlobToListAll(TsImportList[0].TimeStepUnit, TsImportList[0].TimeStepQuantity,
                        TsImportList[0].BlobStartDate, 
                        TsImportList[0].TraceList.Single(t => t.TraceNumber == 1).ValueBlob, ref tsvList);
            Assert.AreEqual(tsvList.Count, 10);
            Assert.AreEqual(tsvList[0].Value, 12.3);
            Assert.AreEqual(tsvList[1].Value, 21.5);
            Assert.AreEqual(tsvList[7].Value, 50.0);
            Assert.AreEqual(tsvList[8].Value, 20.1);
            // Verify BLOB # of Series# 1 Trace# 3 in import list
            TsLib.ConvertBlobToListAll(TsImportList[0].TimeStepUnit, TsImportList[0].TimeStepQuantity,
                        TsImportList[0].BlobStartDate, 
                        TsImportList[0].TraceList.Single(t => t.TraceNumber == 3).ValueBlob, ref tsvList);
            Assert.AreEqual(tsvList.Count, 10);
            Assert.AreEqual(tsvList[0].Value, 32.3);
            Assert.AreEqual(tsvList[1].Value, 51.5);
            Assert.AreEqual(tsvList[2].Value, 52.7);
            Assert.AreEqual(tsvList[9].Value, 50.0);
            // Verify BLOB # of Series# 2 Trace# 1 in import list
            TsLib.ConvertBlobToListAll(TsImportList[1].TimeStepUnit, TsImportList[1].TimeStepQuantity,
                        TsImportList[1].BlobStartDate, 
                        TsImportList[1].TraceList.Single(t => t.TraceNumber == 1).ValueBlob, ref tsvList);
            Assert.AreEqual(tsvList.Count, 17);
            Assert.AreEqual(tsvList[0].Value, 112.5); Assert.AreEqual(tsvList[0].Date, DateTime.Parse("01/01/1930 11:59:00"));
            Assert.AreEqual(tsvList[1].Value, 121.7); Assert.AreEqual(tsvList[5].Date, DateTime.Parse("01/03/1930 23:59:00"));
            Assert.AreEqual(tsvList[3].Value, 399.8); Assert.AreEqual(tsvList[14].Date, DateTime.Parse("01/11/1930 23:59:00"));
            Assert.AreEqual(tsvList[16].Value, 312.2); Assert.AreEqual(tsvList[16].Date, DateTime.Parse("01/15/1930 23:59:00"));
            // Verify BLOB # of Series# 2 Trace# 3 in import list
            TsLib.ConvertBlobToListAll(TsImportList[1].TimeStepUnit, TsImportList[1].TimeStepQuantity,
                        TsImportList[1].BlobStartDate,
                        TsImportList[1].TraceList.Single(t => t.TraceNumber == 3).ValueBlob, ref tsvList);
            Assert.AreEqual(tsvList.Count, 17);
            Assert.AreEqual(tsvList[0].Value, 312.5); Assert.AreEqual(tsvList[0].Date, DateTime.Parse("01/01/1930 11:59:00"));
            Assert.AreEqual(tsvList[1].Value, 321.7); Assert.AreEqual(tsvList[5].Date, DateTime.Parse("01/03/1930 23:59:00"));
            Assert.AreEqual(tsvList[3].Value, 399.8); Assert.AreEqual(tsvList[14].Date, DateTime.Parse("01/11/1930 23:59:00"));
            Assert.AreEqual(tsvList[14].Value, 2312.2); Assert.AreEqual(tsvList[16].Date, DateTime.Parse("01/15/1930 23:59:00"));


            // Verify Units in import list
            Assert.AreEqual(TsImportList[0].Units, "CFS");
            Assert.AreEqual(TsImportList[1].Units, "AFD");
            // Verify TimeSeriesType in import list
            Assert.AreEqual(TsImportList[0].TimeSeriesType, "PER-CUM");
            Assert.AreEqual(TsImportList[1].TimeSeriesType, "INST-VAL");
            // Verify UnprocessedElements in import list
            Assert.AreEqual(TsImportList[0].UnprocessedElements, " <Random>^!@  #()?</Random>");
            Assert.AreEqual(TsImportList[1].UnprocessedElements, " <OldManTag></OldManTag> <Schlap>Spoing-oing-oing</Schlap>");

        } 
        #endregion


        #region Test Methods for error-handling of ReadAndStore()
        // Method should throw exception b/c it cannot write to database w/o having called the
        // constructor that initializes database fields.
        [TestMethod()]
        public void ReadAndStoreDBError()
        {
            string xmlText = Properties.Resources.test1;
            bool storeToDatabase = true; // does not combine with empty constructor
            bool recordDetails = true;

            try
            {
                int ret = TsXml.ReadAndStore(null, xmlText, TsImportList, storeToDatabase, recordDetails);
                Assert.Fail("Should have thrown exception");
            }
            catch (TSLibraryException e)
            {
                Assert.AreEqual(ErrCode.Enum.Xml_Connection_Not_Initialized, e.ErrCode);
            }
        }

        // Method should throw exception b/c it cannot be called with both an XML text
        // and an XML file.
        [TestMethod()]
        public void ReadAndStoreTwoXmlError()
        {
            string xmlText = Properties.Resources.test1;
            bool storeToDatabase = false;
            bool recordDetails = true;

            try
            {
                int ret = TsXml.ReadAndStore("dummy", xmlText, TsImportList, storeToDatabase, recordDetails);
                Assert.Fail("Should have thrown exception");
            }
            catch (TSLibraryException e)
            {
                Assert.AreEqual(ErrCode.Enum.Xml_Memory_File_Exclusion, e.ErrCode);
            }
        }

        // Method should throw exception b/c it cannot be called with both the XML text
        // and the XML set to null.
        [TestMethod()]
        public void ReadAndStoreNoXmlError()
        {
            bool storeToDatabase = false;
            bool recordDetails = true;

            try
            {
                int ret = TsXml.ReadAndStore(null, null, TsImportList, storeToDatabase, recordDetails);
                Assert.Fail("Should have thrown exception");
            }
            catch (TSLibraryException e)
            {
                Assert.AreEqual(ErrCode.Enum.Xml_Memory_File_Exclusion, e.ErrCode);
            }
        }

        // Method should throw exception b/c it has no <Import> tag
        [TestMethod()]
        public void ReadAndStoreMissingTagError1()
        {
            string xmlText = "   "; // simple text that lacks an <Import> tag -- in fact any tags!
            bool storeToDatabase = false;
            bool recordDetails = true;

            try
            {
                int ret = TsXml.ReadAndStore(null, xmlText, TsImportList, storeToDatabase, recordDetails);
                Assert.Fail("Should have thrown exception");
            }
            catch (TSLibraryException e)
            {
                Assert.AreEqual(ErrCode.Enum.Xml_File_Empty, e.ErrCode);
            }
        }

        // Method should throw exception b/c it has no <TimeSeries> tag
        [TestMethod()]
        public void ReadAndStoreMissingTagError2()
        {
            string xmlText = "<Import></Import>";
            bool storeToDatabase = false;
            bool recordDetails = true;

            try
            {
                int ret = TsXml.ReadAndStore(null, xmlText, TsImportList, storeToDatabase, recordDetails);
                Assert.Fail("Should have thrown exception");
            }
            catch (TSLibraryException e)
            {
                Assert.AreEqual(ErrCode.Enum.Xml_File_Empty, e.ErrCode);
            }
        }

        // Method should throw exception b/c it lacks any tags inside the <TimeSeries> element
        [TestMethod()]
        public void ReadAndStoreMissingTagError3()
        {
            string xmlText = "<Import><TimeSeries></TimeSeries></Import>";
            bool storeToDatabase = false;
            bool recordDetails = true;

            try
            {
                int ret = TsXml.ReadAndStore(null, xmlText, TsImportList, storeToDatabase, recordDetails);
                Assert.Fail("Should have thrown exception");
            }
            catch (TSLibraryException e)
            {
                Assert.AreEqual(ErrCode.Enum.Xml_File_Incomplete, e.ErrCode);
            }
        }

        // Method should throw exception b/c it lacks a <Data> tag
        [TestMethod()]
        public void ReadAndStoreMissingTagError4()
        {
            string xmlText =
                "<Import><TimeSeries>" +
                "<StartDate>10/01/1927 03:09:00</StartDate>" +
                "<TimeStepUnit>Hour</TimeStepUnit>" +
                "<TimeStepQuantity>1</TimeStepQuantity>" +
                "</TimeSeries></Import>";
            bool storeToDatabase = false;
            bool recordDetails = true;

            try
            {
                int ret = TsXml.ReadAndStore(null, xmlText, TsImportList, storeToDatabase, recordDetails);
                Assert.Fail("Should have thrown exception");
            }
            catch (TSLibraryException e)
            {
                Assert.AreEqual(ErrCode.Enum.Xml_File_Incomplete, e.ErrCode);
            }
        }
        // Method should not throw any exceptions for missing tags
        // The test method is a backcheck on the MissingTag tests
        [TestMethod()]
        public void ReadAndStoreMissingTagErrorBackCheck()
        {
            string xmlText =
                "<Import><TimeSeries>" +
                " <StartDate>2/28/1927 15:00:00</StartDate>" +
                " <TimeStepUnit>Month</TimeStepUnit>" +
                " <TimeStepQuantity>6</TimeStepQuantity>" +
                " <Data>" +
                "   77.7890" +
                "   -5.5196" +
                " </Data> " +
                "</TimeSeries></Import>";
            bool storeToDatabase = false;
            bool recordDetails = true;

            int ret = TsXml.ReadAndStore(null, xmlText, TsImportList, storeToDatabase, recordDetails);
            // Correct number of items added to import list?
            Assert.AreEqual(TsImportList.Count, 1);
            // Verify TimeStepUnit in import list
            Assert.AreEqual(TsImportList[0].TimeStepUnit, TSDateCalculator.TimeStepUnitCode.Month);
            // Verify TimeStepQuantity in import list
            Assert.AreEqual(TsImportList[0].TimeStepQuantity, 6);
            // Verify TimeStepCount in import list
            Assert.AreEqual(TsImportList[0].TimeStepCount, 2);
            // Verify BlobStartDate in import list
            Assert.AreEqual(TsImportList[0].BlobStartDate, DateTime.Parse("2/28/1927 15:00:00"));
        }
        // method should throw exception because trace number 2 appears twice
        [TestMethod()]
        public void ReadAndStore_DoubleTraceNumError()
        {
            string xmlText =
                "<Import><TimeSeries>" +
                " <StartDate>2/28/1927 15:00:00</StartDate>" +
                " <TimeStepUnit>Month</TimeStepUnit>" +
                " <TimeStepQuantity>6</TimeStepQuantity>" +
                " <Data Trace=\"2\">" +
                "   77.7890" +
                "   -5.5196" +
                " </Data> " +
                " <Data Trace=\"2\">" +
                "   87.7890" +
                "   -8.5196" +
                " </Data> " +
                "</TimeSeries></Import>";
            bool storeToDatabase = false;
            bool recordDetails = true;

            try
            {
                int ret = TsXml.ReadAndStore(null, xmlText, TsImportList, storeToDatabase, recordDetails);
                Assert.Fail("Should have thrown exception");
            }
            catch (TSLibraryException e)
            {
                Assert.AreEqual(ErrCode.Enum.Xml_File_Inconsistent, e.ErrCode);
            }
        }
        // method should throw exception because the traces have unequal time step counts
        [TestMethod()]
        public void ReadAndStore_MismatchedTraceLengthError()
        {
            string xmlText =
                "<Import><TimeSeries>" +
                " <StartDate>2/28/1927 15:00:00</StartDate>" +
                " <TimeStepUnit>Month</TimeStepUnit>" +
                " <TimeStepQuantity>6</TimeStepQuantity>" +
                " <Data Trace=\"1\">" +
                "   77.7890" +
                "   -5.5196" +
                " </Data> " +
                " <Data Trace=\"2\">" +
                "   87.7890" +
                "   -8.5196" +
                "   -3.5196" +
                " </Data> " +
                "</TimeSeries></Import>";
            bool storeToDatabase = false;
            bool recordDetails = true;

            try
            {
                int ret = TsXml.ReadAndStore(null, xmlText, TsImportList, storeToDatabase, recordDetails);
                Assert.Fail("Should have thrown exception");
            }
            catch (TSLibraryException e)
            {
                Assert.AreEqual(ErrCode.Enum.Xml_File_Inconsistent, e.ErrCode);
            }
        }
        #endregion



        #region Test Methods for ParseTimeStepUnit()
        // The method should successfully parse strings that match the names of the enum
        [TestMethod()]
        public void ParseTimeStepUnit_EnumNames()
        {
            TSDateCalculator.TimeStepUnitCode timeStepUnit;

            timeStepUnit = TsXml.ParseTimeStepUnit("Minute");
            Assert.AreEqual(timeStepUnit, TSDateCalculator.TimeStepUnitCode.Minute);

            timeStepUnit = TsXml.ParseTimeStepUnit("Hour");
            Assert.AreEqual(timeStepUnit, TSDateCalculator.TimeStepUnitCode.Hour);

            timeStepUnit = TsXml.ParseTimeStepUnit("Day");
            Assert.AreEqual(timeStepUnit, TSDateCalculator.TimeStepUnitCode.Day);

            timeStepUnit = TsXml.ParseTimeStepUnit("Week");
            Assert.AreEqual(timeStepUnit, TSDateCalculator.TimeStepUnitCode.Week);

            timeStepUnit = TsXml.ParseTimeStepUnit("Month");
            Assert.AreEqual(timeStepUnit, TSDateCalculator.TimeStepUnitCode.Month);

            timeStepUnit = TsXml.ParseTimeStepUnit("Year");
            Assert.AreEqual(timeStepUnit, TSDateCalculator.TimeStepUnitCode.Year);
        }
        // The method should successfully parse strings that match the patterns
        // used by HECDSS record name E parts, including the abbreviations for
        // Month and Minute.
        [TestMethod()]
        public void ParseTimeStepUnit_DssStyle()
        {
            TSDateCalculator.TimeStepUnitCode timeStepUnit;

            timeStepUnit = TsXml.ParseTimeStepUnit("Min");
            Assert.AreEqual(timeStepUnit, TSDateCalculator.TimeStepUnitCode.Minute);

            timeStepUnit = TsXml.ParseTimeStepUnit("2Min");
            Assert.AreEqual(timeStepUnit, TSDateCalculator.TimeStepUnitCode.Minute);

            timeStepUnit = TsXml.ParseTimeStepUnit("720Min");
            Assert.AreEqual(timeStepUnit, TSDateCalculator.TimeStepUnitCode.Minute);

            timeStepUnit = TsXml.ParseTimeStepUnit("7Minute");
            Assert.AreEqual(timeStepUnit, TSDateCalculator.TimeStepUnitCode.Minute);

            timeStepUnit = TsXml.ParseTimeStepUnit("3HOUR");
            Assert.AreEqual(timeStepUnit, TSDateCalculator.TimeStepUnitCode.Hour);

            timeStepUnit = TsXml.ParseTimeStepUnit("1DAY");
            Assert.AreEqual(timeStepUnit, TSDateCalculator.TimeStepUnitCode.Day);

            timeStepUnit = TsXml.ParseTimeStepUnit("30day");
            Assert.AreEqual(timeStepUnit, TSDateCalculator.TimeStepUnitCode.Day);

            timeStepUnit = TsXml.ParseTimeStepUnit("8Week");
            Assert.AreEqual(timeStepUnit, TSDateCalculator.TimeStepUnitCode.Week);

            timeStepUnit = TsXml.ParseTimeStepUnit("80WEEK");
            Assert.AreEqual(timeStepUnit, TSDateCalculator.TimeStepUnitCode.Week);

            timeStepUnit = TsXml.ParseTimeStepUnit("200Week");
            Assert.AreEqual(timeStepUnit, TSDateCalculator.TimeStepUnitCode.Week);

            timeStepUnit = TsXml.ParseTimeStepUnit("Mon");
            Assert.AreEqual(timeStepUnit, TSDateCalculator.TimeStepUnitCode.Month);

            timeStepUnit = TsXml.ParseTimeStepUnit("8MON");
            Assert.AreEqual(timeStepUnit, TSDateCalculator.TimeStepUnitCode.Month);

            timeStepUnit = TsXml.ParseTimeStepUnit("23Month");
            Assert.AreEqual(timeStepUnit, TSDateCalculator.TimeStepUnitCode.Month);

            timeStepUnit = TsXml.ParseTimeStepUnit("YEAR");
            Assert.AreEqual(timeStepUnit, TSDateCalculator.TimeStepUnitCode.Year);

            timeStepUnit = TsXml.ParseTimeStepUnit("6Year");
            Assert.AreEqual(timeStepUnit, TSDateCalculator.TimeStepUnitCode.Year);

            timeStepUnit = TsXml.ParseTimeStepUnit("year");
            Assert.AreEqual(timeStepUnit, TSDateCalculator.TimeStepUnitCode.Year);
        }
        // The method should throw exceptions for strings that can not be parsed
        [TestMethod()]
        public void ParseTimeStepUnit_Err()
        {
            TSDateCalculator.TimeStepUnitCode timeStepUnit;

            try
            {
                timeStepUnit = TsXml.ParseTimeStepUnit("7282118");
                Assert.Fail("Should have thrown exception");
            }
            catch (TSLibraryException e)
            {
                Assert.AreEqual(ErrCode.Enum.Xml_Unit_Name_Unrecognized, e.ErrCode);
            }
            try
            {
                timeStepUnit = TsXml.ParseTimeStepUnit("Totally Wrong");
                Assert.Fail("Should have thrown exception");
            }
            catch (TSLibraryException e)
            {
                Assert.AreEqual(ErrCode.Enum.Xml_Unit_Name_Unrecognized, e.ErrCode);
            }
            try
            {
                timeStepUnit = TsXml.ParseTimeStepUnit("Monster");
                Assert.Fail("Should have thrown exception");
            }
            catch (TSLibraryException e)
            {
                Assert.AreEqual(ErrCode.Enum.Xml_Unit_Name_Unrecognized, e.ErrCode);
            }
            try
            {
                timeStepUnit = TsXml.ParseTimeStepUnit("Minotaur");
                Assert.Fail("Should have thrown exception");
            }
            catch (TSLibraryException e)
            {
                Assert.AreEqual(ErrCode.Enum.Xml_Unit_Name_Unrecognized, e.ErrCode);
            }
        }
        #endregion


        #region Test Methods for ParseTimeStepQuantity()
        // The method should parse simple integers
        [TestMethod()]
        public void ParseTimeStepQuantity_Proper()
        {
            short timeStepQuantity;

            timeStepQuantity = TsXml.ParseTimeStepQuantity("3");
            Assert.AreEqual(timeStepQuantity, 3);

            timeStepQuantity = TsXml.ParseTimeStepQuantity("29");
            Assert.AreEqual(timeStepQuantity, 29);

            timeStepQuantity = TsXml.ParseTimeStepQuantity("177");
            Assert.AreEqual(timeStepQuantity, 177);

        }
        // The method should successfully parse strings that match the patterns
        // used by HECDSS record name E parts.
        [TestMethod()]
        public void ParseTimeStepQuantity_DssStyle()
        {
            short timeStepQuantity;

            timeStepQuantity = TsXml.ParseTimeStepQuantity("4Day");
            Assert.AreEqual(timeStepQuantity, 4);

            timeStepQuantity = TsXml.ParseTimeStepQuantity("19Min");
            Assert.AreEqual(timeStepQuantity, 19);

            timeStepQuantity = TsXml.ParseTimeStepQuantity("177Bozos");
            Assert.AreEqual(timeStepQuantity, 177);
        }
        // The method should throw exceptions for strings that can not be parsed
        [TestMethod()]
        public void ParseTimeStepQuantity_Err()
        {
            short timeStepQuantity;

            try
            {
                timeStepQuantity = TsXml.ParseTimeStepQuantity("ointment");
                Assert.Fail("Should have thrown exception");
            }
            catch (TSLibraryException e)
            {
                Assert.AreEqual(ErrCode.Enum.Xml_Quantity_Unrecognized, e.ErrCode);
            }
            try
            {
                timeStepQuantity = TsXml.ParseTimeStepQuantity("()");
                Assert.Fail("Should have thrown exception");
            }
            catch (TSLibraryException e)
            {
                Assert.AreEqual(ErrCode.Enum.Xml_Quantity_Unrecognized, e.ErrCode);
            }
        } 
        #endregion
    
    }
}
