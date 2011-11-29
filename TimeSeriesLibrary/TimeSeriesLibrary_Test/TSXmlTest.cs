using TimeSeriesLibrary;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;

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


        #region Test Method for ReadAndStore() without errors
        [TestMethod()]
        public void ReadAndStoreTest()
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

            // Verify Checksum in import list
            // The checksums are verified against previous values, so the test is only as good as the
            // original trial.  However, it is good if the test flags any change, to ensure that the
            // developer can account for any change.  E.g., if the original value was incorrect, then
            // that should be documented and the new output should be scrutinized.
            Assert.AreEqual(BitConverter.ToString(TsImportList[0].Checksum), "18-40-AD-03-C4-26-FE-2F-A5-21-5A-1A-A4-27-CF-5B");
            Assert.AreEqual(BitConverter.ToString(TsImportList[1].Checksum), "18-58-54-B8-B7-BC-B5-4A-B3-C1-BB-EC-44-2E-81-7F");
            Assert.AreEqual(BitConverter.ToString(TsImportList[2].Checksum), "A9-E0-24-D3-B7-E3-6F-29-73-08-AA-2A-AC-4A-64-5D");

            List<TimeSeriesValue> tsvList = null;
            // Verify BLOB # 1 in import list
            TsLib.ConvertBlobToListAll(TsImportList[0].TimeStepUnit, TsImportList[0].TimeStepQuantity,
                        TsImportList[0].BlobStartDate, TsImportList[0].BlobData,
                        ref tsvList);
            Assert.AreEqual(tsvList.Count, 9);
            Assert.AreEqual(tsvList[0].Value, 12.3);
            Assert.AreEqual(tsvList[1].Value, 21.5);
            Assert.AreEqual(tsvList[7].Value, 20.1);
            Assert.AreEqual(tsvList[8].Value, 12.4);
            // Verify BLOB # 2 in import list
            TsLib.ConvertBlobToListAll(TsImportList[1].TimeStepUnit, TsImportList[1].TimeStepQuantity,
                        TsImportList[1].BlobStartDate, TsImportList[1].BlobData,
                        ref tsvList);
            Assert.AreEqual(tsvList.Count, 11);
            Assert.AreEqual(tsvList[0].Value, 12.5);
            Assert.AreEqual(tsvList[1].Value, 21.7);
            Assert.AreEqual(tsvList[9].Value, 12.2);
            Assert.AreEqual(tsvList[10].Value, 12.6);
            // Verify BLOB # 3 in import list
            TsLib.ConvertBlobToListAll(TsImportList[2].TimeStepUnit, TsImportList[2].TimeStepQuantity,
                        TsImportList[2].BlobStartDate, TsImportList[2].BlobData,
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
            Assert.AreEqual(TsImportList[0].TraceNumber, 2);
            Assert.AreEqual(TsImportList[1].TraceNumber, 5);
            Assert.AreEqual(TsImportList[2].TraceNumber, 22);
            // Verify UnprocessedElements in import list
            Assert.AreEqual(TsImportList[1].UnprocessedElements, null);
            Assert.AreEqual(TsImportList[2].UnprocessedElements, " <OldManTag></OldManTag> <Schlap>Spoing-oing-oing</Schlap>");

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
        #endregion
    
    }
}
