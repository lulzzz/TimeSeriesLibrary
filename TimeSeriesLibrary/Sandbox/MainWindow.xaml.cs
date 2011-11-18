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
        const int nVals = 30000, nIter = 500;

        int connNumber;
        TSLibrary tsLib = new TSLibrary();
        Guid testId1 = new Guid();
        Guid testId2 = new Guid();
        DateTime StartDate = new DateTime(1928, 1, 1, 23, 59, 0);

        public MainWindow()
        {
            InitializeComponent();

            connNumber = tsLib.OpenConnection(
                "Data Source=.; Database=OasisOutput; Trusted_Connection=yes;");

            //HashTest();
            //WriteOneSeriesIrreg();
            //WriteOneSeriesArray();
            //ReadOneSeriesModel();
            WriteOneSeriesList();
            //ReadOneSeriesArray();
        }
        private void MainWindowClosed(object sender, EventArgs e)
        {
            tsLib.CloseConnection(connNumber);
        }
        

        private void GoButtonClick(object sender, RoutedEventArgs e)
        {
            //ImportTest();
            //ReadArrayTest();
            //ReadListTest();
            //WriteArrayTest();
            //WriteListTest();
            //DeleteTest();
            //HashTimer();

        }

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

        //    TimeSeriesValue[] dateValueArray = new TimeSeriesValue[nVals];

        //    ret = tsLib.ReadDatesValuesUnsafe(connNumber, "FileStrm2",
        //                    testId1, nVals, dateValueArray, StartDate, StartDate.AddDays(5));

        //    ret = tsLib.ReadDatesValuesUnsafe(connNumber, "FileStrm2",
        //                    testId2, nVals, dateValueArray, StartDate, StartDate.AddDays(3));

        //    ret = 3;
        //}
        void ReadOneSeriesGUI()
        {
            int ret;

            List<TimeSeriesValue> dateValueList = new List<TimeSeriesValue>();

            ret = tsLib.ReadAllDatesValues(connNumber, "FileStrm2",
                            testId1, ref dateValueList);

            ret = tsLib.ReadAllDatesValues(connNumber, "FileStrm2",
                            testId2, ref dateValueList);

            ret = tsLib.ReadLimitedDatesValues(connNumber, "FileStrm2",
                            testId1, nVals, ref dateValueList, StartDate, StartDate.AddDays(3));

            ret = tsLib.ReadLimitedDatesValues(connNumber, "FileStrm2",
                            testId2, nVals, ref dateValueList, StartDate, StartDate.AddDays(3));

            ret = 3;
        }

        void DeleteTest()
        {
            bool ret = tsLib.DeleteMatchingSeries(connNumber, "FileStrm2", "Id > 102476");
            if (ret == false)
            {
            }
        }

        void ImportTest()
        {
            int i;
            List<TSImport> l = new List<TSImport>();

            DateTime timerStart = DateTime.Now;
            for (i = 0; i < nIter; i++)
            {
                tsLib.XmlImportWithList(connNumber, "FileStrm2",
                            "D:\\OASIS\\_Build\\TimeSeriesLibrary\\TimeSeriesLibrary\\Sandbox\\test2irreg.xml", l);
            }
            DateTime timerEnd = DateTime.Now;
            TimeSpan timerDiff = timerEnd - timerStart;
            TimeLabelBlob.Content = String.Format("Imported --- Iterations: {0};  Duration: {1:hh\\:mm\\:ss\\.f}", i, timerDiff);
        }

        void WriteOneSeriesIrreg()
        {
            int i;
            DateTime date = StartDate;

            TimeSeriesValue[] dateValArray = new TimeSeriesValue[nVals];
            TS ts = new TS(tsLib.ConnxObject.TSConnectionsCollection[connNumber], "FileStrm2");

            for (i = 0; i < nVals; i++)
            {
                dateValArray[i].Date = date;
                dateValArray[i].Value = i*5;
                
                date = date.AddDays(1);
            }
            testId1 = ts.WriteValuesIrregular(nVals, dateValArray);
            
            TimeSeriesValue[] outArray = new TimeSeriesValue[nVals];
            i = ts.ReadValuesIrregular(testId1, nVals, outArray, StartDate, dateValArray[nVals-1].Date);
            date = StartDate;
        }
        //void WriteOneSeriesArray()
        //{
        //    int i;

        //    double[] valArray = new double[nVals];

        //    for (i = 0; i < nVals; i++)
        //        valArray[i] = i * 3;
        //    testId2 = tsLib.WriteValuesRegularUnsafe(connNumber, "FileStrm2",
        //               3, 1, nVals, valArray, StartDate);

        //    //List<TimeSeriesValue> tsvList = new List<TimeSeriesValue>();
        //    //tsLib.ReadDatesValues(connNumber, "FileStrm2", testId1,
        //    //            nVals, ref tsvList, StartDate);

        //    DateTime date = StartDate;

        //}
        void WriteOneSeriesList()
        {
            int i;

            List<double> valList = new List<double>();
            List<TimeSeriesValue> dateValList = new List<TimeSeriesValue>();
            DateTime date = StartDate;

            TimeSeriesValue tsv;
            for (i = 0; i < nVals; i++)
            {
                valList.Add(i * 10);
                tsv.Date = date;
                tsv.Value = i * 10;
                dateValList.Add(tsv);
                date = date.AddDays(3);
            }

            //testId2 = tsLib.WriteValuesRegular(connNumber, "FileStrm2",
            //           3, 1, nVals, valList, StartDate);

            //testId2 = tsLib.WriteValues(connNumber, "FileStrm2",
            //                (short)TSDateCalculator.TimeStepUnitCode.Irregular, 0, dateValList);

            testId2 = tsLib.WriteValues(connNumber, "FileStrm2",
                            (short)TSDateCalculator.TimeStepUnitCode.Day, 3, dateValList);

            
            List<TimeSeriesValue> dv = new List<TimeSeriesValue>();
            i = tsLib.ReadAllDatesValues(connNumber, "FileStrm2", testId2, ref dv);
            i = 3;
        }
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
        //void ReadListTest()
        //{
        //    int ret, i;

        //    List<double> valList = new List<double>();

        //    DateTime timerStart = DateTime.Now;
        //    for (i = 0; i < nIter; i++)
        //    {
        //        TimeLabelBlob.Content = String.Format("Iteration {0}", i);
        //        ret = tsLib.ReadValuesRegular(connNumber, "FileStrm2",
        //                        testId1, nVals, ref valList, StartDate);
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
        void WriteListTest()
        {
            int i;

            List<double> valList = new List<double>();

            for (i = 0; i < nVals; i++)
                valList.Add(i * 1.5);

            DateTime timerStart = DateTime.Now;
            for (i = 0; i < nIter; i++)
            {
                TimeLabelBlob.Content = String.Format("Iteration {0}", i);
                tsLib.WriteValuesRegular(connNumber, "FileStrm2",
                           3, 1, nVals, valList, StartDate);
            }
            DateTime timerEnd = DateTime.Now;
            TimeSpan timerDiff = timerEnd - timerStart;
            TimeLabelBlob.Content = String.Format("BLOBWRI --- Iterations: {0};  Duration: {1:hh\\:mm\\:ss\\.f}", i, timerDiff);
        }

        
        
    }
}
