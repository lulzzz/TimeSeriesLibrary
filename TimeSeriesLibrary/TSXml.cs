using System;
using System.Linq;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Text;
using System.IO;
using System.Xml;

namespace TimeSeriesLibrary
{
    /// <summary>
    /// This class is used to import data from an XML file into the TimeSeriesLibrary.
    /// </summary>
    public class TSXml
    {
        public ErrCodes.Enum ErrorCode;

        private String TableName;
        private SqlConnection Connx;

        #region Class Constructor
        /// <summary>
        /// Class constructor
        /// </summary>
        /// <param name="connx">SqlConnection object that this object will use</param>
        /// <param name="tableName">Name of the table in the database that stores this object's records</param>
        public TSXml(SqlConnection connx, String tableName)
        {
            Connx = connx;
            TableName = tableName;
        }
        #endregion

        /// <summary>
        /// This method reads the given XML file and stores each of the timeseries described
        /// therein to the database.
        /// </summary>
        /// <param name="xmlFileName">Name of the file that will be read</param>
        /// <param name="tsImportList">List of TSImport objects that this function records for each series that is imported.
        /// This function appends the list.</param>
        /// <returns>The number of time series records that were successfully stored</returns>
        public int ReadAndStore(String xmlFileName, List<TSImport> tsImportList)
        {
            String s;
            int numTs = 0;

            using (XmlReader xmlReader = XmlReader.Create(xmlFileName))
            {
                // All of the data that we'll read is contained inside an element named 'Import'
                if (!xmlReader.ReadToFollowing("Import"))
                    return 0;
                // The file must contain at least one element named 'TimeSeries'.  Move to the first
                // such element now.
                if (!xmlReader.ReadToDescendant("TimeSeries"))
                    // if no such element is found then there is nothing to process
                    return 0;
                // do-while loop through all elements named 'TimeSeries'
                do
                {
                    // Get a new XmlReader object that can not read outside of the current 'TimeSeries' element.
                    XmlReader oneSeriesXmlReader = xmlReader.ReadSubtree();
                    // A new TSImport object will store properties of this time series that the TimeSeriesLibrary
                    // is not designed to handle.
                    TSImport tsImport = new TSImport();
                    // Declare the values that we'll be reading for this time series
                    TSDateCalculator.TimeStepUnitCode TimeStepUnit = TSDateCalculator.TimeStepUnitCode.Day;
                    short TimeStepQuantity = 1;
                    DateTime StartDate = DateTime.Parse("1/1/2000");
                    double[] valueArray = null;

                    // advance the reader past the outer element
                    oneSeriesXmlReader.ReadStartElement();
                    //      Read one timeseries from XML
                    while (oneSeriesXmlReader.Read())
                    {
                        if (oneSeriesXmlReader.NodeType == XmlNodeType.Element)
                        {
                            switch (oneSeriesXmlReader.Name)
                            {
                                case "Name":
                                    tsImport.Name = oneSeriesXmlReader.ReadElementContentAsString();
                                    break;

                                case "StartDate":
                                    s = oneSeriesXmlReader.ReadElementContentAsString();
                                    StartDate = DateTime.Parse(s);
                                    break;

                                case "TimeStepUnit":
                                    s = oneSeriesXmlReader.ReadElementContentAsString();
                                    TimeStepUnit = (TSDateCalculator.TimeStepUnitCode)
                                                        Enum.Parse(typeof(TSDateCalculator.TimeStepUnitCode), s);
                                    break;

                                case "TimeStepQuantity":
                                    TimeStepQuantity = (short)oneSeriesXmlReader.ReadElementContentAsInt();
                                    break;

                                case "Data":
                                    //char[] buffer = new char[1048576];
                                    //oneSeriesXmlReader.MoveToContent();
                                    //int bufferLength = oneSeriesXmlReader.ReadValueChunk(buffer, 0, 1048576);
                                    s = oneSeriesXmlReader.ReadElementContentAsString();
                                    valueArray = s.Split(new char[] { }, StringSplitOptions.RemoveEmptyEntries)
                                                    .Select(z => double.Parse(z)).ToArray();
                                    break;

                                default:
                                    tsImport.UnprocessedElements += oneSeriesXmlReader.ReadOuterXml();
                                    break;
                            }
                        }
                    }
                    oneSeriesXmlReader.Close();
                    // add the tsImport object to the collection
                    TS ts = new TS(Connx, TableName);
                    ts.WriteValues((short)TimeStepUnit, TimeStepQuantity, valueArray.Length, valueArray, StartDate);
                    tsImport.Id = ts.Id;
                    ts = null;
                    tsImportList.Add(tsImport);
                    numTs++;
                
                } while (xmlReader.ReadToNextSibling("TimeSeries"));
            }
            return numTs;
        }

    }
}
