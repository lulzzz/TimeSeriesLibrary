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
        public ErrCode.Enum ErrorCode;

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

        #region Method ReadAndStore
        /// <summary>
        /// This method reads the given XML file and stores each of the timeseries described
        /// therein to the database.  For each timeseries that it stores, it adds an item to
        /// list 'tsImportList', which records metadata of the timeseries that is not processed
        /// directly by TimeSeriesLibrary.  Therefore, the process that calls TimeSeriesLibrary
        /// can process tsImportList to complete the importation of the timeseries.
        /// </summary>
        /// <param name="xmlFileName">Name of the file that will be read</param>
        /// <param name="tsImportList">List of TSImport objects that this function records for each series that is imported.
        /// This function appends the list.</param>
        /// <returns>The number of time series records that were successfully stored</returns>
        public int ReadAndStore(String xmlFileName, List<TSImport> tsImportList)
        {
            String s;       // ephemeral String object
            String DataString="";  // String that holds the unparsed DataSeries
            int numTs = 0;  // The # of time series successfuly processed by this method
            
            TSDateCalculator.TimeStepUnitCode TimeStepUnit = TSDateCalculator.TimeStepUnitCode.Day; // to be read from XML
            short TimeStepQuantity = 1;                       // to be read from XML
            DateTime StartDate = DateTime.Parse("1/1/2000");  // to be read from XML
            double[] valueArray = null;                       // to be read from XML

            // Flags will indicate if the XML is missing any data
            Boolean foundTimeStepUnit, foundTimeStepQuantity, foundStartDate, foundValueArray;

            // This XmlReader object opens the XML file and parses it for us.  The 'using'
            // statement ensures that the XmlReader's resources are properly disposed.
            using (XmlReader xmlReader = XmlReader.Create(xmlFileName))
            {
                // All of the data that we'll read is contained inside an element named 'Import'
                if (!xmlReader.ReadToFollowing("Import"))
                    throw new TSLibraryException(ErrCode.Enum.Xml_File_Empty, 
                                "The XML file '"+xmlFileName+"' does not contain an <Import> element.");
                // The file must contain at least one element named 'TimeSeries'.  Move to the first
                // such element now.
                if (!xmlReader.ReadToDescendant("TimeSeries"))
                    // if no such element is found then there is nothing to process
                    throw new TSLibraryException(ErrCode.Enum.Xml_File_Empty,
                                "The XML file '" + xmlFileName + "' does not contain any <TimeSeries> elements.");
                // do-while loop through all elements named 'TimeSeries'.  There will be one iteration
                // of this loop for each timeseries in the XML file.
                do
                {
                    // Get a new XmlReader object that can not read outside of the current 'TimeSeries' element.
                    XmlReader oneSeriesXmlReader = xmlReader.ReadSubtree();
                    // A new TSImport object will store properties of this time series that the TimeSeriesLibrary
                    // is not designed to handle.
                    TSImport tsImport = new TSImport();
                    // Flags will indicate if the XML is missing any data
                    foundTimeStepUnit = foundTimeStepQuantity = foundStartDate = foundValueArray = false;

                    // advance the reader past the outer element
                    oneSeriesXmlReader.ReadStartElement();
                    //      Read one timeseries from XML
                    while (oneSeriesXmlReader.Read())
                    {
                        // If the current position of the reader is on an elements start tag (e.g. <Name>)
                        if (oneSeriesXmlReader.NodeType == XmlNodeType.Element)
                        {
                            // Note that XML standard is case sensitive
                            switch (oneSeriesXmlReader.Name)
                            {
                                case "Name":
                                    // <Name> is not processed by TimeSeriesLibrary.  Record it on a list so another
                                    // module can process the <Name> field.  Presumably, the process will distinguish
                                    // timeseries based on the <Name> field.
                                    tsImport.Name = oneSeriesXmlReader.ReadElementContentAsString();
                                    break;

                                case "StartDate":
                                    // TimeSeriesLibrary will store <StartDate> to the data table
                                    s = oneSeriesXmlReader.ReadElementContentAsString();
                                    StartDate = DateTime.Parse(s);
                                    foundStartDate = true;
                                    break;

                                case "TimeStepUnit":
                                    // TimeSeriesLibrary will store <TimeStepUnit> to the data table
                                    s = oneSeriesXmlReader.ReadElementContentAsString();
                                    TimeStepUnit = (TSDateCalculator.TimeStepUnitCode)
                                                        Enum.Parse(typeof(TSDateCalculator.TimeStepUnitCode), s);
                                    foundTimeStepUnit = true;
                                    // If it is an irregular time series
                                    if (TimeStepUnit == TSDateCalculator.TimeStepUnitCode.Irregular)
                                    {
                                        // <TimeStepQuantity> and <StartDate> are unnecessary and irrelevant
                                        // to irregular time series
                                        foundTimeStepQuantity = true;
                                        foundStartDate = true;
                                    }
                                    break;

                                case "TimeStepQuantity":
                                    // TimeSeriesLibrary will store <TimeStepQuantity> to the data table
                                    TimeStepQuantity = (short)oneSeriesXmlReader.ReadElementContentAsInt();
                                    foundTimeStepQuantity = true;
                                    break;

                                case "Data":
                                    // <Data> contains a whitespace-deliminted string of values that comprise the time series
                                    DataString = oneSeriesXmlReader.ReadElementContentAsString();
                                    foundValueArray = true;
                                    break;

                                default:
                                    // Any other tags are simply copied to the String object 'UnprocessedElements'.
                                    // Here they are stored with the enclosing tags (e.g. "<Units>CFS</Units>").
                                    tsImport.UnprocessedElements += oneSeriesXmlReader.ReadOuterXml();
                                    break;
                            }
                        }
                    }
                    // This XmlReader object was created with the ReadSubtree() method so that it would only
                    // be able to read the current time series element.  We have now reached the end of the
                    // time series element, so the XmlReader should be closed.
                    oneSeriesXmlReader.Close();
                    // The record can not be saved to the table if information for some of the fields is missing.
                    // These flags indicate whether each of the required fields was found in the XML file.
                    if (foundTimeStepUnit && foundTimeStepQuantity && foundStartDate && foundValueArray)
                    {
                    }
                    else
                    {
                        // One or more required fields were missing, so we'll throw an exception.
                        String errorList, nameString;
                        if(tsImport.Name=="") 
                            nameString = "unnamed time series";
                        else 
                            nameString = "time series named '" + tsImport.Name + "'";
                        errorList = "Some required subelements were missing from " + nameString + " in file " + xmlFileName + "\n";
                        if (!foundStartDate) errorList += "\n<StartDate> was not found";
                        if (!foundTimeStepUnit) errorList += "\n<TimeStepUnit> was not found";
                        if (!foundTimeStepQuantity) errorList += "\n<TimeStepQuantity> was not found";
                        if (!foundValueArray) errorList += "\n<Data> was not found";
                        throw new TSLibraryException(ErrCode.Enum.Xml_File_Incomplete, errorList);
                    }
                    // Now that we've established that all fields have been read, we can parse the 
                    // string of timeseries values into an array, and save the array to the database.
                    if (TimeStepUnit == TSDateCalculator.TimeStepUnitCode.Irregular)
                    {
                        // Split the big data string into an array of strings.  The date/time/value triplets will be
                        // all collated together.
                        String[] stringArray = DataString.Split(new char[] { }, StringSplitOptions.RemoveEmptyEntries);
                        // We'll use this date/value structure to build each item of the date/value array
                        TSDateValueStruct tsv = new TSDateValueStruct();
                        // allocate the array of date/value pairs
                        TSDateValueStruct[] dateValueArray = new TSDateValueStruct[stringArray.Length/3];
                        // Loop through the array of strings, 3 elements at a time
                        for (int i = 2; i < stringArray.Length; i += 3)
                        {
                            s = stringArray[i - 2] + " " + stringArray[i - 1];
                            tsv.Date = DateTime.Parse(s);
                            tsv.Value = double.Parse(stringArray[i]);
                            dateValueArray[i/3] = tsv;
                        }
                        // The TS object is used to save one record to the database table
                        TS ts = new TS(Connx, TableName);
                        // save the record
                        tsImport.Id = ts.WriteValuesIrregular(dateValueArray.Length, dateValueArray);
                        // Done with the TS object.
                        ts = null;
                    }
                    else
                    {
                        // Fancy LINQ statement turns the String object into an array of double[]
                        valueArray = DataString.Split(new char[] { }, StringSplitOptions.RemoveEmptyEntries)
                                        .Select(z => double.Parse(z)).ToArray();
                        // The TS object is used to save one record to the database table
                        TS ts = new TS(Connx, TableName);
                        // save the record
                        tsImport.Id = ts.WriteValuesRegular((short)TimeStepUnit, TimeStepQuantity,
                                                valueArray.Length, valueArray, StartDate);
                        // Done with the TS object.
                        ts = null;
                    }
                    
                    // the TSImport object contains data for this timeseries that TSLibrary does not process.
                    // Add the TSImport object to a list that the calling process can read and use.
                    tsImportList.Add(tsImport);
                    numTs++;
                
                } while (xmlReader.ReadToNextSibling("TimeSeries"));
            }
            return numTs;
        }
    	#endregion 

    }
}
