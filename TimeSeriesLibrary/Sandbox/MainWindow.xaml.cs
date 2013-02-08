using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Windows.Navigation;
using System.Windows.Shapes;

using System.Data;
using System.Data.SqlClient;

using System.IO;

using System.Security.Cryptography;

using TimeSeriesLibrary;

namespace Sandbox
{
    /// <summary>
    /// Interaction logic for MainWindow.xaml
    /// </summary>
    public partial class MainWindow : Window
    {
        const int nVals = 30000, nIter = 400;

        int connNumber;
        TSLibrary tsLib = new TSLibrary();
        //ComTSLibrary ctsLib = new ComTSLibrary();
        int testId1;
        int testId2;
        DateTime StartDate = new DateTime(1928, 1, 1, 23, 59, 0);

        public MainWindow()
        {
            InitializeComponent();

            connNumber = tsLib.OpenConnection(
                "Data Source=.; Database=HardDriveTS; Trusted_Connection=yes;");

            //WriteArrayTest();
            //ImportTest();
            //HashTest();
            //WriteOneSeriesIrreg();
            //WriteOneSeriesArray();
            //ReadOneSeriesModel();
            //WriteOneSeriesList();
            //ReadOneSeriesArray();
        }
        private void MainWindowClosed(object sender, EventArgs e)
        {
            //ctsLib.CloseConnection(connNumber);
            tsLib.CloseConnection(connNumber);
        }
        

        private void GoButtonClick(object sender, RoutedEventArgs e)
        {
            try
            {
                //ImportTest();
                //ReadArrayTest();
                //ReadListTest(true, true);
                //WriteArrayTest();
                RunDataTableExp();
                //DeleteTest();
                //HashTimer();
                //CompressionTimeTrial(true, true);
            }
            catch (Exception exc)
            {
                MessageBox.Show(exc.Message);
            }

        }

        void RunDataTableExp()
        {
            // Start the timer
            DateTime timerStart = DateTime.Now;

            var d = new DataTableExperiment();
            d.Connx = tsLib.GetConnectionFromId(connNumber);
            d.Test(false, true);

            // Stop the timer
            DateTime timerEnd = DateTime.Now;
            TimeSpan timerDiff = timerEnd - timerStart;
            TimeLabelBlob.Content = String.Format("DONE --- Duration: {0:hh\\:mm\\:ss\\.f}", timerDiff);
        }

        #region CompressionTimeTrial
        ///// <summary>
        ///// This method was developed in order to carefully time the speed effect of doing compression on time
        ///// series.  The method reads all output timeseries from a previously executed run.  The characteristics of
        ///// the different time series in the run lead to different compression ratios, and therefore different
        ///// speed effects.  Therefore, this test does not time just one or a few hypothetical time series, but rather
        ///// it determines the effect on a whole set of time series that are actually in use.
        ///// </summary>
        ///// <param name="shouldUseDB">If true, then the method will record the time to actually read and write
        ///// the time series to database.  If fasle, the method only converts the value arrays to and from BLOB</param>
        ///// <param name="shouldUseHardDrive">If true, the method will switch to the database that is on hard drive</param>
        //void CompressionTimeTrial(Boolean shouldUseDB, Boolean shouldUseHardDrive)
        //{
        //    Dictionary<TS, double[]> tsList = new Dictionary<TS, double[]>();
        //    SqlConnection connx = tsLib.GetConnectionFromId(connNumber);
        //    // read all timeseries from a particular run, store in TS objects
        //    String comm = String.Format("select Id from OutputTimeSeries where RunGUID='28005d8e-9966-433a-982a-7b786bbf0cfc'");
        //    using (SqlDataAdapter adp = new SqlDataAdapter(comm, connx))
        //    using (DataTable dTable = new DataTable())
        //    {
        //        try
        //        {
        //            adp.Fill(dTable);
        //        }
        //        catch (Exception e)
        //        {   throw new TSLibraryException(ErrCode.Enum.Could_Not_Open_Table,
        //                            "Table 'OutputTimeSeries' could not be opened using query:\n\n" + comm, e);
        //        }
        //        if (dTable.Rows.Count < 1)
        //        {
        //            throw new TSLibraryException(ErrCode.Enum.Record_Not_Found_Table,
        //                        "Found zero records using query:\n\n." + comm);
        //        }
        //        foreach (DataRow dataRow in dTable.Rows)
        //        {
        //            int id = dataRow.Field<int>("Id");
        //            TS ts = new TS(connx, "OutputTimeSeries", "OutputTimeSeriesTraces");
        //            if (!ts.IsInitialized) ts.Initialize(id);
        //            double[] valueArray = new double[ts.TimeStepCount];
        //            ts.ReadValuesRegular(id, 1, ts.TimeStepCount, valueArray, ts.BlobStartDate, ts.BlobEndDate, false, false);
        //            tsList.Add(ts, valueArray);
        //            TimeLabelBlob.Content = dTable.Rows.IndexOf(dataRow).ToString();
        //        }

        //    }
        //    if (shouldUseHardDrive)
        //    {
        //        // This was done because I have my main database on SSD, but I wanted to test the speed
        //        // of a database on hard drive.  Here we switch to the HD database.

        //        // I temporarily made property TS.Connx public so that the below code would work

        //        //tsLib.CloseConnection(connNumber);
        //        //connNumber = tsLib.OpenConnection(
        //        //    "Data Source=.; Database=HardDriveTS; Trusted_Connection=yes;");
        //        //foreach (TS ts in tsList.Keys)
        //        //    ts.Connx = tsLib.GetConnectionFromId(connNumber);
        //    }

        //    TimeLabelBlob.Content = "";
        //    StreamWriter outfile = new StreamWriter("Compress.csv");

        //    String extraParamNames = "TimeSeriesType, Unit_Id, RunGUID, VariableType, VariableName, RunElementGUID";
        //    String extraParamValues = "22, 1, '00000000-0000-0000-0000-000000000000', 'XXX', 'XXX', '00000000-0000-0000-0000-000000000000'";

        //    Boolean hasLZFX, hasZlib;  int zlibCompressionLevel;
        //    DateTime timerStart, timerEnd;  TimeSpan timerDiff;  String spanString, labelString;
        //    // loop thru several compression options
        //    for (int optionIndex = 0; optionIndex < 4; optionIndex++)
        //    {
        //        switch (optionIndex)
        //        {
        //            case 0:
        //                hasLZFX = false;
        //                hasZlib = false;
        //                zlibCompressionLevel = 1;
        //                break;
        //            case 1:
        //                hasLZFX = true;
        //                hasZlib = false;
        //                zlibCompressionLevel = 1;
        //                break;
        //            case 2:
        //                hasLZFX = true;
        //                hasZlib = true;
        //                zlibCompressionLevel = 1;
        //                break;
        //            case 3:
        //                hasLZFX = false;
        //                hasZlib = true;
        //                zlibCompressionLevel = 1;
        //                break;
        //            default:
        //                throw new NotImplementedException();
        //        }
        //        Dictionary<TS, Tuple<double, byte[]>> resultList = new Dictionary<TS, Tuple<double, byte[]>>();
        //        labelString = "COMPRESS LZFX=" + hasLZFX.ToString() + " zlib=" + hasZlib.ToString()
        //                        + " lev=" + zlibCompressionLevel.ToString();
        //        timerStart = DateTime.Now;
        //        // foreach TS object, compress it into a collection of blobs
        //        // record time and compression ratios to file
        //        foreach (TS ts in tsList.Keys)
        //        {
        //            double[] valueArray = tsList[ts];
        //            if (shouldUseDB)
        //            {
        //                int id = ts.WriteParametersRegular(true, null, 
        //                            (short)ts.TimeStepUnit, ts.TimeStepQuantity, ts.TimeStepCount, ts.BlobStartDate,
        //                            extraParamNames, extraParamValues);
        //                ts.WriteTraceRegular(id, true, null, 1, valueArray, hasLZFX, hasZlib, zlibCompressionLevel);
        //            }
        //            else
        //            {
        //                byte[] blob = TSBlobCoder.ConvertArrayToBlobRegular(ts.TimeStepCount, valueArray, hasLZFX, hasZlib, zlibCompressionLevel);
        //                float uncompressedSize = valueArray.Length * sizeof(double);
        //                float compressedSize = blob.Length;
        //                float compressionRatio = compressedSize / uncompressedSize;
        //                resultList.Add(ts, new Tuple<double, byte[]>(compressionRatio, blob));
        //            }
        //        }
        //        timerEnd = DateTime.Now;
        //        timerDiff = timerEnd - timerStart;
        //        spanString = String.Format(" {0:hh\\:mm\\:ss\\.f}", timerDiff);
        //        TimeLabelBlob.Content += spanString + "\n";

        //        outfile.WriteLine(labelString);
        //        if (!shouldUseDB)
        //        {
        //            foreach (TS ts in tsList.Keys)
        //            {
        //                outfile.WriteLine(resultList[ts].Item1.ToString("0.0000"));
        //            }
        //            outfile.WriteLine("");
        //        }
        //        outfile.WriteLine(spanString);
        //        outfile.WriteLine("");
        //        outfile.WriteLine("");


        //        labelString = "DECOMPRS LZFX=" + hasLZFX.ToString() + " zlib=" + hasZlib.ToString()
        //                        + " lev=" + zlibCompressionLevel.ToString();
        //        timerStart = DateTime.Now;
        //        // foreach blob in the collection, decompress
        //        // record time to file
        //        foreach (TS ts in tsList.Keys)
        //        {
        //            if (shouldUseDB)
        //            {
        //                List<TimeSeriesValue> valueList = new List<TimeSeriesValue>();
        //                tsLib.ReadAllDatesValues(connNumber, "OutputTimeSeries", "OutputTimeSeriesTraces",
        //                        ts.Id, 1, ref valueList, hasLZFX, hasZlib);
        //            }
        //            else
        //            {
        //                double[] valueArray = new double[ts.TimeStepCount];
        //                byte[] blob = resultList[ts].Item2;
        //                TSBlobCoder.ConvertBlobToArrayRegular(ts.TimeStepUnit, ts.TimeStepQuantity,
        //                                ts.TimeStepCount, ts.BlobStartDate,
        //                                false, ts.TimeStepCount, ts.BlobStartDate, ts.BlobEndDate,
        //                                blob, valueArray,
        //                                hasLZFX, hasZlib);
        //            }
        //        }
        //        timerEnd = DateTime.Now;
        //        timerDiff = timerEnd - timerStart;
        //        spanString = String.Format(" {0:hh\\:mm\\:ss\\.f}", timerDiff);
        //        TimeLabelBlob.Content += spanString + "\n";

        //        outfile.WriteLine(labelString);
        //        outfile.WriteLine(spanString);
        //        outfile.WriteLine("");
        //        outfile.WriteLine("");
        //    }
        //    outfile.Close();

        //}
        
        #endregion
        /// <summary>
        /// 
        /// </summary>
        //void ReadListTest(Boolean hasLZFXcompression, Boolean hasZlibCompression)
        //{
        //    int ret, i;

        //    List<TimeSeriesValue> valList = new List<TimeSeriesValue>();

        //    DateTime timerStart = DateTime.Now;
        //    for (i = 0; i < nIter; i++)
        //    {
        //        //TimeLabelBlob.Content = String.Format("Iteration {0}", i);
        //        ret = tsLib.ReadAllDatesValues(connNumber,
        //                "OutputTimeSeries", "OutputTimeSeriesTraces",
        //                70055, 1, ref valList,
        //                hasLZFXcompression, hasZlibCompression);
        //    }
        //    DateTime timerEnd = DateTime.Now;
        //    TimeSpan timerDiff = timerEnd - timerStart;
        //    TimeLabelBlob.Content = String.Format("BLOBBED --- Iterations: {0};  Duration: {1:hh\\:mm\\:ss\\.f}", i, timerDiff);
        //}
        void WriteListTest()
        {
            int i;

            List<List<double>> valList = new List<List<double>>();
            String extraParamNames = "TimeSeriesType, Unit_Id, RunGUID, VariableType, VariableName, RunElementGUID";
            String extraParamValues = "22, 1, '00000000-0000-0000-0000-000000000000', 'XXX', 'XXX', '00000000-0000-0000-0000-000000000000'";

            // Create dummy time series that we can write to the database
            for (i = 0; i < nIter; i++)
            {
                var iterList = new List<double>();
                valList.Add(iterList);
                for (int t = 0; t < nVals; t++)
                {
                    iterList.Add(1.5 * t + i + 0.33);
                }
            }

            // Start the timer
            DateTime timerStart = DateTime.Now;
            // Write to the database
            for (i = 0; i < nIter; i++)
            {
                TimeLabelBlob.Content = String.Format("Iteration {0}", i);
                TS ts = new TS(tsLib.GetConnectionFromId(connNumber), tsLib.ConnxObject,
                        "OutputTimeSeries", "OutputTimeSeriesTraces");
                int id = ts.WriteParametersRegular(true, null, (short)TSDateCalculator.TimeStepUnitCode.Day, 1, nVals, StartDate,
                        extraParamNames, extraParamValues);
                ts.WriteTraceRegular(id, true, null, 1, valList[i].ToArray());
            }
            // Stop the timer
            DateTime timerEnd = DateTime.Now;
            TimeSpan timerDiff = timerEnd - timerStart;
            TimeLabelBlob.Content = String.Format("BLOBWRI --- Iterations: {0};  Duration: {1:hh\\:mm\\:ss\\.f}", i, timerDiff);
        }

        //void WriteArrayTest()
        //{
        //    int i;

        //    double[] valArray = new double[nVals];

        //    for (i = 0; i < nVals; i++)
        //        valArray[i] = i * 3;

        //    for (i = 0; i < nIter; i++)
        //    {
        //        int id = ctsLib.WriteParametersRegularUnsafe(connNumber, "RunOutputTimeSeries", "OutputTimeSeriesTraces",
        //                   3, 1, nVals, StartDate,
        //                   new String[6] { "RunGUID", "VariableType", "VariableName", "TimeSeriesType", "RunElementGUID", "Unit_Id" },
        //                   new String[6] { "'EF8A01FE-C250-429C-A3AF-160076DE142B'", "'E'", "'D'", "1", "'EF8A01FE-C250-429C-A3AF-160076DE142B'", "1" });

        //        for (int t = 1; t <= 3; t++)
        //            ctsLib.WriteTraceRegularUnsafe(connNumber, "RunOutputTimeSeries", "OutputTimeSeriesTraces",
        //                        id, t, valArray);
        //    }
        //}


        
        void HashTest()
        {
            byte[] inArray1 = new ASCIIEncoding().GetBytes("PartA");
            byte[] inArray2 = new ASCIIEncoding().GetBytes("PartB");
            byte[] inArray3 = new ASCIIEncoding().GetBytes("PartAPartC");
            byte[] hash1 =new byte[16], hash2 = new byte[16];

            MD5CryptoServiceProvider md5Hasher = new MD5CryptoServiceProvider();

            md5Hasher.TransformBlock(inArray1, 0, inArray1.Length, inArray1, 0);
            md5Hasher.TransformFinalBlock(inArray2, 0, inArray2.Length);
            hash1 = md5Hasher.Hash;

            md5Hasher = new MD5CryptoServiceProvider();
            md5Hasher.TransformFinalBlock(inArray3, 0, inArray3.Length);
            hash2 = md5Hasher.Hash;
            

        }
        void HashTimer()
        {
            byte[] inArray1 = new byte[24];
            byte[] inArray2 = new byte[30000*sizeof(double)];
            int i;

            for (i = 0; i < 24; i++)
                inArray1[i] = (byte)(23 + i*2);

            MemoryStream binStream = new MemoryStream(inArray2);
            BinaryWriter binWriter = new BinaryWriter(binStream);
            for (i = 0; i < 30000; i++)
                binWriter.Write(i * (double)2.2);

            DateTime timerStart = DateTime.Now;
            for (i = 0; i < nIter; i++)
            {
                MD5CryptoServiceProvider md5Hasher = new MD5CryptoServiceProvider();
                md5Hasher.TransformBlock(inArray1, 0, inArray1.Length, inArray1, 0);
                md5Hasher.TransformFinalBlock(inArray2, 0, inArray2.Length);
                byte[] hash1 = md5Hasher.Hash;
            }
            DateTime timerEnd = DateTime.Now;
            TimeSpan timerDiff = timerEnd - timerStart;
            TimeLabelBlob.Content = String.Format("ChkSum --- Iterations: {0};  Duration: {1:hh\\:mm\\:ss\\.f}", i, timerDiff);

            i = 3;

        }


        //void ReadOneSeriesModel()
        //{
        //    int ret;

        //    TSDateValueStruct[] dateValueArray = new TSDateValueStruct[nVals];

        //    ret = tsLib.ReadDatesValuesUnsafe(connNumber, "FileStrm2",
        //                    testId1, nVals, dateValueArray, StartDate, StartDate.AddDays(5));

        //    ret = tsLib.ReadDatesValuesUnsafe(connNumber, "FileStrm2",
        //                    testId2, nVals, dateValueArray, StartDate, StartDate.AddDays(3));

        //    ret = 3;
        //}
        //void ReadOneSeriesGUI()
        //{
        //    int ret;

        //    List<TimeSeriesValue> dateValueList = new List<TimeSeriesValue>();

        //    ret = tsLib.ReadAllDatesValues(connNumber, "FileStrm2",
        //                    testId1, ref dateValueList);

        //    ret = tsLib.ReadAllDatesValues(connNumber, "FileStrm2",
        //                    testId2, ref dateValueList);

        //    ret = tsLib.ReadLimitedDatesValues(connNumber, "FileStrm2",
        //                    testId1, nVals, ref dateValueList, StartDate, StartDate.AddDays(3));

        //    ret = tsLib.ReadLimitedDatesValues(connNumber, "FileStrm2",
        //                    testId2, nVals, ref dateValueList, StartDate, StartDate.AddDays(3));

        //    ret = 3;
        //}

        //void DeleteTest()
        //{
        //    bool ret = tsLib.DeleteMatchingSeries(connNumber, "FileStrm2", "Id > 102476");
        //    if (ret == false)
        //    {
        //    }
        //}

        void ImportTest()
        {
            int i;
            List<TSImport> l = new List<TSImport>();
            tsLib.XmlImport("D:\\temp\\TimeSeriesLibrary\\basedata_11-22.dss.xml", l);
            i = 2;

            //DateTime timerStart = DateTime.Now;
            //for (i = 0; i < nIter; i++)
            //{
            //    tsLib.XmlImport("D:\temp\TimeSeriesLibrary\basedata_11-22.dss.xml", l);
            //}
            //DateTime timerEnd = DateTime.Now;
            //TimeSpan timerDiff = timerEnd - timerStart;
            //TimeLabelBlob.Content = String.Format("Imported --- Iterations: {0};  Duration: {1:hh\\:mm\\:ss\\.f}", i, timerDiff);
        }

        //void WriteOneSeriesIrreg()
        //{
        //    int i;
        //    DateTime date = StartDate;

        //    TSDateValueStruct[] dateValArray = new TSDateValueStruct[nVals];
        //    TS ts = new TS(tsLib.ConnxObject.TSConnectionsCollection[connNumber], "FileStrm2");

        //    for (i = 0; i < nVals; i++)
        //    {
        //        dateValArray[i].Date = date;
        //        dateValArray[i].Value = i*5;
                
        //        date = date.AddDays(1);
        //    }
        //    testId1 = ts.WriteValuesIrregular(true, null, nVals, dateValArray);
            
        //    TSDateValueStruct[] outArray = new TSDateValueStruct[nVals];
        //    i = ts.ReadValuesIrregular(testId1, nVals, outArray, StartDate, dateValArray[nVals-1].Date);
        //    date = StartDate;
        //}
        //void WriteOneSeriesArray()
        //{
        //    int i;

        //    double[] valArray = new double[nVals];

        //    for (i = 0; i < nVals; i++)
        //        valArray[i] = i * 3;
        //    testId2 = tsLib.WriteValuesRegularUnsafe(connNumber, "FileStrm2",
        //               3, 1, nVals, valArray, StartDate);

        //    //List<TSDateValueStruct> tsvList = new List<TSDateValueStruct>();
        //    //tsLib.ReadDatesValues(connNumber, "FileStrm2", testId1,
        //    //            nVals, ref tsvList, StartDate);

        //    DateTime date = StartDate;

        //}
        //void WriteOneSeriesList()
        //{
        //    int i;

        //    List<double> valList = new List<double>();
        //    List<TimeSeriesValue> dateValList = new List<TimeSeriesValue>();
        //    DateTime date = StartDate;

        //    for (i = 0; i < nVals; i++)
        //    {
        //        valList.Add(i * 10);
        //        dateValList.Add(new TimeSeriesValue { Date = date, Value = i * 10 });
        //        date = date.AddDays(3);
        //    }

        //    //testId2 = tsLib.WriteValuesRegular(connNumber, "FileStrm2",
        //    //           3, 1, nVals, valList, StartDate);

        //    testId1 = tsLib.WriteValues(connNumber, "FileStrm2",
        //                    (short)TSDateCalculator.TimeStepUnitCode.Irregular, 0, dateValList);

        //    testId2 = tsLib.WriteValues(connNumber, "FileStrm2",
        //                    (short)TSDateCalculator.TimeStepUnitCode.Day, 3, dateValList);


        //    List<TimeSeriesValue> dv = new List<TimeSeriesValue>();
        //    i = tsLib.ReadAllDatesValues(connNumber, "FileStrm2", testId1, ref dv);
        //    i = 3;

        //    dv = new List<TimeSeriesValue>();
        //    i = tsLib.ReadAllDatesValues(connNumber, "FileStrm2", testId2, ref dv);
        //    i = 3;
        //}
        //void ReadOneSeriesArray()
        //{
        //    int ret;

        //    double[] valArray = new double[nVals];

        //    ret = tsLib.ReadValuesRegularUnsafe(connNumber, "FileStrm2",
        //                    testId1, nVals, valArray, StartDate);

        //    ret = tsLib.ReadValuesRegularUnsafe(connNumber, "FileStrm2",
        //                    testId2, nVals, valArray, StartDate);

        //    ret = 3;
        //}

        //void ReadArrayTest()
        //{
        //    int ret, i;

        //    double[] valArray = new double[nVals];

        //    DateTime timerStart = DateTime.Now;
        //    for (i = 0; i < nIter; i++)
        //    {
        //        TimeLabelBlob.Content = String.Format("Iteration {0}", i);
        //        ret = tsLib.ReadValuesRegularUnsafe(connNumber, "FileStrm2",
        //                        testId1, nVals, valArray, StartDate);
        //    }
        //    DateTime timerEnd = DateTime.Now;
        //    TimeSpan timerDiff = timerEnd - timerStart;
        //    TimeLabelBlob.Content = String.Format("BLOBBED --- Iterations: {0};  Duration: {1:hh\\:mm\\:ss\\.f}", i, timerDiff);
        //}


        //void WriteArrayTest()
        //{
        //    int i;

        //    double[] valArray = new double[nVals];

        //    for (i = 0; i < nVals; i++)
        //        valArray[i] = i * 3;

        //    DateTime timerStart = DateTime.Now;
        //    for (i = 0; i < nIter; i++)
        //    {
        //        TimeLabelBlob.Content = String.Format("Iteration {0}", i);
        //        tsLib.WriteValuesRegularUnsafe(connNumber, "FileStrm2",
        //                   3, 1, nVals, valArray, StartDate);
        //    }
        //    DateTime timerEnd = DateTime.Now;
        //    TimeSpan timerDiff = timerEnd - timerStart;
        //    TimeLabelBlob.Content = String.Format("BLOBWRI --- Iterations: {0};  Duration: {1:hh\\:mm\\:ss\\.f}", i, timerDiff);
        //}

        
        
    }
}
