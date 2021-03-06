﻿using System;
using System.Linq;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Text;
using System.Text.RegularExpressions;
using System.IO;
using System.Xml;
using Oasis.Foundation.Infrastructure;

namespace TimeSeriesLibrary
{
    /// <summary>
    /// This class is used to import data from an XML file into the TimeSeriesLibrary.
    /// </summary>
    public class TSXml
    {
        private String TableName;
        private String TraceTableName;
        private TSConnection TSConnection;
        private DateTime _defaultTime = new DateTime(2000, 1, 1, 23, 59, 59);

        private String reportedFileName;
        public String ReportedFileName
        {
            get
            {
                if (reportedFileName == null) return "The XML file ";
                else return reportedFileName;
            }
            set{ reportedFileName = value; }
        }


        #region Class Constructor
        /// <summary>
        /// Class constructor that should be invoked if the XML object will save to the database
        /// </summary>
        /// <param name="connx">SqlConnection object that this object will use</param>
        /// <param name="tableName">Name of the table in the database that stores this object's records</param>
        /// <param name="traceTableName">The name of the database table that stores the BLOB for a single trace</param>
        public TSXml(TSConnection connx, String tableName, String traceTableName)
        {
            TSConnection = connx;
            TableName = tableName;
            TraceTableName = traceTableName;
        }
        /// <summary>
        /// Class constructor that should be invoked if the XML object will NOT save to the database
        /// </summary>
        public TSXml()
        {
            TSConnection = null;
            TableName = null;
        }
        #endregion


        #region Method ReadAndStore()
        /// <summary>
        /// This method reads the given XML file and stores each of the timeseries described
        /// therein to the database.  For each timeseries that it stores, it adds an item to
        /// list 'tsImportList', which records metadata of the timeseries that is not processed
        /// directly by TimeSeriesLibrary.  Therefore, the process that calls TimeSeriesLibrary
        /// can process tsImportList to complete the importation of the timeseries.
        /// </summary>
        /// <param name="xmlFileName">Name of the file that will be read.  If xmlText is null,
        /// then this parameter must be non-null, and vice-versa.</param>
        /// <param name="xmlText">The text of an XML file that will be read.  If xmlFileName is null,
        /// then this parameter must be non-null, and vice-versa.</param>
        /// <param name="tsImportList">List of TSImport objects that this function records for 
        /// each series that is imported.  This method appends the list.</param>
        /// <param name="shouldStoreToDatabase">If true, then this method will write the timeseries from 
        /// the XML file to database.  If false, then this method does not write to database.</param>
        /// <param name="shouldRecordDetails">If true, then this method stores the BLOB and detailed
        /// elements to the list of TSImport objects.  If false, then this method does not store
        /// the BLOB to the TSImport object, and all fields that TimeSeriesLibrary does not process
        /// are stored to the TSImport object's UnprocessedElements field.</param>
        /// <returns>The number of time series records that were successfully stored</returns>
        public int ReadAndStore(
                        String xmlFileName, String xmlText,
                        List<TSImport> tsImportList,
                        Boolean shouldStoreToDatabase, Boolean shouldRecordDetails)
        {
            String s;       // ephemeral String object
            int numTs = 0;  // The # of time series successfuly processed by this method
            
            TSDateCalculator.TimeStepUnitCode TimeStepUnit = TSDateCalculator.TimeStepUnitCode.Day; // to be read from XML
            short TimeStepQuantity = 1;                       // to be read from XML
            DateTime StartDate = _defaultTime;                // to be read from XML
            double[] valueArray = null;                       // to be read from XML
            var elementNames = new List<String> { "TimeSeries", "Pattern" };

            // Flags will indicate if the XML is missing any data
            Boolean foundTimeStepUnit, foundTimeStepQuantity, foundStartDate, foundValueArray;

            // Error checks
            if (xmlFileName == null && xmlText == null)
                throw new TSLibraryException(ErrCode.Enum.Xml_Memory_File_Exclusion,
                            "The method's xmlFileName and xmlText parameters can not both be null.");
            if (xmlFileName != null && xmlText != null)
                throw new TSLibraryException(ErrCode.Enum.Xml_Memory_File_Exclusion,
                            "The method's xmlFileName and xmlText parameters can not both be non-null.");
            if(shouldStoreToDatabase && TSConnection==null)
                throw new TSLibraryException(ErrCode.Enum.Xml_Connection_Not_Initialized,
                            "The method is directed to store results to database, " +
                            "but a database connection has not been assigned in the constructor.");

            // Initialize a Stream object for the XmlReader object to read from.  This method can
            // be called with either the file name of an XML file, or with a string containing the
            // complete text of the XML to be parsed.  The type of Stream object that we initialize
            // depends on which parameter the method was called with.
            Stream xmlStream;
            if (xmlFileName == null)
            {
                xmlStream = new MemoryStream(Encoding.ASCII.GetBytes(xmlText));
                ReportedFileName = "The given XML text ";
            }
            else
            {
                xmlStream = new FileStream(xmlFileName, FileMode.Open);
                ReportedFileName = "The XML file '" + xmlFileName + "' ";
            }

            try
            {
                // Loop once for "TimeSeries" and once for "Pattern".  Note that the approach of looping
                // through the entire file twice seems undesirable, but no better option was evident because
                // methods of XmlReader such as 'ReadToNextSibling' do not have an equivalent that could
                // seek *either* "TimeSeries" or "Pattern".
                foreach (String elementName in elementNames)
                {
                    Boolean isPattern = elementName == "Pattern";
                    // Start at the beginning of the XML file
                    xmlStream.Seek(0, SeekOrigin.Begin);
                    // This XmlReader object opens the XML file and parses it for us.  The 'using'
                    // statement ensures that the XmlReader's resources are properly disposed.
                    using (XmlReader xmlReader = XmlReader.Create(xmlStream))
                    {
                        // All of the data that we'll read is contained inside an element named 'Import'
                        try
                        {
                            if (!xmlReader.ReadToFollowing("Import"))
                                throw new TSLibraryException(ErrCode.Enum.Xml_File_Empty,
                                            ReportedFileName + "does not contain an <Import> element.");
                        }
                        catch
                        {
                            throw new TSLibraryException(ErrCode.Enum.Xml_File_Empty,
                                        ReportedFileName + "does not contain an <Import> element.");
                        }
                        // The file must contain at least one element named 'TimeSeries'.  Move to the first
                        // such element now.
                        if (!xmlReader.ReadToDescendant(elementName))
                            // if no such element is found then there is nothing to process in this iteration
                            // of the loop.  After the loop is finished, an exception is thrown if no elements
                            // were read.
                            continue;
                        // do-while loop through all elements named 'TimeSeries'.  There will be one iteration
                        // of this loop for each timeseries in the XML file.
                        do
                        {
                            // Get a new XmlReader object that can not read outside 
                            // of the current 'TimeSeries' element.
                            XmlReader oneSeriesXmlReader = xmlReader.ReadSubtree();
                            // A new TSImport object will store properties of this time series 
                            // that the TimeSeriesLibrary is not designed to handle.
                            TSImport tsImport = new TSImport(shouldRecordDetails) { IsPattern = isPattern };
                            // Flags will indicate if the XML is missing any data
                            foundTimeStepUnit = foundTimeStepQuantity = foundStartDate = foundValueArray = false;
                            // This default trace number will be used if the "Trace" attribute is not used on a <Data>
                            // element. The default trace value may be reassigned if a <TraceNumber> element is read.
                            int defaultTraceNumber = 1;
                            // Collection of Strings that hold the unparsed DataSeries.  The key of the
                            // Dictionary is the trace number of the DataSeries
                            Dictionary<int, String> DataStrings = new Dictionary<int, String>();

                            // advance the reader past the outer element
                            oneSeriesXmlReader.ReadStartElement();
                            // Read one timeseries from XML
                            while (oneSeriesXmlReader.Read())
                            {
                                Boolean binaryEncoded = false;
                                // If the current position of the reader is on an element's start tag (e.g. <Name>)
                                if (oneSeriesXmlReader.NodeType == XmlNodeType.Element)
                                {
                                    // Note that XML standard is case sensitive
                                    switch (oneSeriesXmlReader.Name)
                                    {
                                        case "Name":
                                            // <Name> is not processed by TimeSeriesLibrary.  Record it on a list
                                            // so another module can process the <Name> field.  Presumably, 
                                            // the process will distinguish timeseries based on the <Name> field.
                                            tsImport.Name = oneSeriesXmlReader.ReadElementContentAsString();
                                            break;

                                        case "StartDate":
                                            // TimeSeriesLibrary will store <StartDate> to the data table
                                            s = oneSeriesXmlReader.ReadElementContentAsString();
                                            if (!TimeExtensions.TryParse(s, out StartDate, _defaultTime))
                                            {
                                                throw new TSLibraryException(ErrCode.Enum.Xml_File_StartDate_Unreadable,
                                                            ReportedFileName 
                                                        + "contains unreadable StartDate element value " + s);
                                            }
                                            foundStartDate = true;
                                            break;

                                        case "TimeStepUnit":
                                            // TimeSeriesLibrary will store <TimeStepUnit> to the data table
                                            s = oneSeriesXmlReader.ReadElementContentAsString();
                                            TimeStepUnit = ParseTimeStepUnit(s);
                                            foundTimeStepUnit = true;
                                            // If it is an irregular time series
                                            if (TimeStepUnit == TSDateCalculator.TimeStepUnitCode.Irregular)
                                            {
                                                // <TimeStepQuantity> and <StartDate> are unnecessary 
                                                // and irrelevant to irregular time series
                                                foundTimeStepQuantity = true;
                                                foundStartDate = true;
                                            }
                                            break;

                                        case "TimeStepQuantity":
                                            // TimeSeriesLibrary will store <TimeStepQuantity> to the data table
                                            s = oneSeriesXmlReader.ReadElementContentAsString();
                                            TimeStepQuantity = ParseTimeStepQuantity(s);
                                            foundTimeStepQuantity = true;
                                            break;

                                        case "Data":
                                            // <Data> may have a TraceNumber attribute
                                            int traceNumber = 0;
                                            s = oneSeriesXmlReader.GetAttribute("Trace");
                                            if (int.TryParse(s, out traceNumber) == false)
                                                traceNumber = 0;
                                            if (DataStrings.ContainsKey(traceNumber))
                                            {
                                                throw new TSLibraryException(ErrCode.Enum.Xml_File_Inconsistent,
                                                    ReportedFileName + "contains a time series with more than "
                                                        + "one trace number " + traceNumber.ToString());
                                            }
                                            // <Data> contains a whitespace-deliminted string of values 
                                            // that comprise the time series
                                            s = oneSeriesXmlReader.ReadElementContentAsString();
                                            // add the unparsed string to a dictionary 
                                            // where the trace number is the dictionary key
                                            DataStrings.Add(traceNumber, s);
                                            foundValueArray = true;
                                            break;

                                        case "Encoding":
                                            // The default is that <Data> contains decimal text.
                                            // However, the <Encoding> element may specify that it is
                                            // Base64 encoded.
                                            s = oneSeriesXmlReader.ReadElementContentAsString();
                                            if (s == "Base64" || s == "base64")
                                                binaryEncoded = true;
                                            break;

                                        case "Apart": // <Apart> contains the A part of record name from a HECDSS file
                                            tsImport.SetAPart(oneSeriesXmlReader); break;

                                        case "Bpart": // <Bpart> contains the B part of record name from a HECDSS file
                                            tsImport.SetBPart(oneSeriesXmlReader); break;

                                        case "Cpart": // <Cpart> contains the C part of record name from a HECDSS file
                                            tsImport.SetCPart(oneSeriesXmlReader); break;

                                        case "Epart": // <Epart> contains the E part of record name from a HECDSS file
                                            tsImport.SetEPart(oneSeriesXmlReader); break;

                                        case "Units": // <Units> contains the name of the units of measurement for the time series values
                                            tsImport.SetUnits(oneSeriesXmlReader); break;

                                        case "TimeSeriesType":
                                            // <TimeSeriesType> contains the text name of the time series type,
                                            // [PER-AVER | PER-CUM | INST-VAL | INST-CUM]
                                            tsImport.SetTimeSeriesType(oneSeriesXmlReader); break;

                                        case "TraceNumber": // <TraceNumber> contains the trace number for an ensemble
                                            defaultTraceNumber = tsImport.GetTraceNumber(oneSeriesXmlReader); break;

                                        case "MultiplicationFactor":
                                            tsImport.SetMultiplicationFactor(oneSeriesXmlReader); break;

                                        default:
                                            // Any other tags are simply copied to the String object 
                                            // 'UnprocessedElements'. Here they are stored with 
                                            // the enclosing tags (e.g. "<Units>CFS</Units>").
                                            tsImport.AddUnprocessedElement(oneSeriesXmlReader.ReadOuterXml());
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
                            if (!(foundTimeStepUnit && foundTimeStepQuantity && foundStartDate && foundValueArray))
                            {
                                // One or more required fields were missing, so we'll throw an exception.
                                String errorList, nameString;
                                if (tsImport.Name == "")
                                    nameString = "unnamed time series";
                                else
                                    nameString = "time series named '" + tsImport.Name + "'";
                                errorList = "Some required subelements were missing from " + nameString
                                                + " in " + ReportedFileName + "\n";
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
                                // IRREGULAR TIME SERIES

                                // The TS object is used to save one record to the database table
                                TS ts = new TS(TSConnection, TableName, TraceTableName);
                                foreach (KeyValuePair<int, String> keyValuePair in DataStrings)
                                {
                                    int traceNumber = keyValuePair.Key;
                                    if (traceNumber == 0) traceNumber = defaultTraceNumber;
                                    // Split the big data string into an array of strings.  
                                    // The date/time/value triplets will be all collated together.
                                    String[] stringArray = keyValuePair.Value
                                                .Split(new char[] { }, StringSplitOptions.RemoveEmptyEntries);
                                    // We'll use this date/value structure to build each item of the date/value array
                                    TSDateValueStruct tsv = new TSDateValueStruct();
                                    // allocate the array of date/value pairs
                                    TSDateValueStruct[] dateValueArray = new TSDateValueStruct[stringArray.Length / 3];
                                    // Loop through the array of strings, 3 elements at a time
                                    for (int i = 2; i < stringArray.Length; i += 3)
                                    {
                                        s = stringArray[i - 2] + " " + stringArray[i - 1];
                                        tsv.Date = DateTime.Parse(s);
                                        tsv.Value = double.Parse(stringArray[i]);
                                        dateValueArray[i / 3] = tsv;
                                    }
                                    if (tsImport.TraceList.Count == 0)
                                    {
                                        // Ignore whatever was entered in the StartDate element, since it
                                        // might conflict with the date/value entries
                                        StartDate = dateValueArray[0].Date;
                                        // Write parameters to the database and record values in the TSImport object
                                        ts.WriteParametersIrregular(shouldStoreToDatabase, tsImport,
                                                dateValueArray.Count(), StartDate, dateValueArray.Last().Date,
                                                null, null);
                                    }
                                    else
                                    {
                                        if (StartDate != ts.BlobStartDate)
                                            throw new TSLibraryException(ErrCode.Enum.Xml_File_Inconsistent,
                                                ReportedFileName + "contains a time series "
                                                + "with traces that do not overlay each other.");
                                    }
                                    ts.WriteTraceIrregular(0, shouldStoreToDatabase, tsImport,
                                            traceNumber, dateValueArray);
                                }
                                tsImport.RecordFromTS(ts);
                                // Done with the TS object.
                                ts = null;
                            }
                            else
                            {
                                // REGULAR TIME SERIES

                                // The TS object is used to save one record to the database table
                                TS ts = new TS(TSConnection, TableName, TraceTableName);
                                foreach (KeyValuePair<int, String> keyValuePair in DataStrings)
                                {
                                    int traceNumber = keyValuePair.Key;
                                    if (traceNumber == 0) traceNumber = defaultTraceNumber;
                                    // Fancy LINQ statement turns the String object into an array of double[]
                                    valueArray = keyValuePair.Value
                                                    .Split(new char[] { }, StringSplitOptions.RemoveEmptyEntries)
                                                    .Select(z => double.Parse(z)).ToArray();
                                    if (tsImport.TraceList.Count == 0)
                                    {
                                        // Write to the database and record values in the TSImport object
                                        ts.WriteParametersRegular(shouldStoreToDatabase, tsImport,
                                                (short)TimeStepUnit, TimeStepQuantity, valueArray.Count(),
                                                StartDate, null, null);
                                    }
                                    ts.WriteTraceRegular(0, shouldStoreToDatabase, tsImport,
                                            traceNumber, valueArray);
                                }
                                tsImport.RecordFromTS(ts);
                                // Done with the TS object.
                                ts = null;
                            }

                            // the TSImport object contains data for this timeseries that TSLibrary does not process.
                            // Add the TSImport object to a list that the calling process can read and use.
                            tsImportList.Add(tsImport);
                            numTs++;

                        } while (xmlReader.ReadToNextSibling(elementName));
                    }
                }
                if (numTs == 0)
                    throw new TSLibraryException(ErrCode.Enum.Xml_File_Empty,
                            ReportedFileName + " does not contain any "
                          + elementNames.Select(ss => "<" + ss + ">").ToStringWithConjunc("or")
                          + " element".Pluralize(elementNames.Count) + ".");
            }
            catch (XmlException e)
            {
                // An XmlException was caught somewhere in the lifetime of the object xmlReader,
                // so we can presumably say there was an error in how the XML file was formatted.
                // The information from the XmlException object is included in the error message
                // that we throw here, and the XmlException is included as an inner exception.
                throw new TSLibraryException(ErrCode.Enum.Xml_File_Malformed,
                            ReportedFileName + "is malformed.\n\n" + e.Message, e);
            }
            finally
            {
                // Disposing the Stream object ensures that the file is closed, if applicable.
                // This is put into the 'finally' clause so that we can ensure that the Stream
                // is closed no matter how we exit this method.
                xmlStream.Dispose();
            }
            
            return numTs;
        }
    	#endregion 


        #region Method ParseTimeStepUnit() 
        /// <summary>
        /// This method returns the TimeStepUnitCode from the given string.  The method
        /// will first attempt to parse the given string as a valid TimeStepUnitCode.
        /// If that fails, then it attempts to parse the given string as a HECDSS record
        /// name E part.  If that fails, then an exception is thrown.
        /// </summary>
        /// <param name="s">The string that is to be parsed to a TimeStepUnitCode</param>
        /// <returns>TimeStepUnitCode that results from the given string</returns>
        public TSDateCalculator.TimeStepUnitCode ParseTimeStepUnit(String s)
        {
            try
            {   // First assume that the string is the name of a TimeStepUnitCode enum.
                // If that fails then we'll try some more complicated assumptions.
                return (TSDateCalculator.TimeStepUnitCode)
                             Enum.Parse(typeof(TSDateCalculator.TimeStepUnitCode), s, true);
            }
            catch { }

            try
            {
                // Assume that the string is a number followed by the unit name.
                // This pattern would be followed by data that came straight from 
                // HECDSS record name E part.
                String substring = Regex.Match(s, @"(\D)+").Value;
                // Ensure that it is in title case (first letter is capital, others are lower case).
                // An exception will be thrown (and caught) if extracted string is empty.
                substring = substring.Substring(0, 1).ToUpper() + substring.Substring(1).ToLower();

                // Check for abbreviated unit names that are used by HECDSS
                if (substring == "Min")
                    return TSDateCalculator.TimeStepUnitCode.Minute;
                if (substring == "Mon")
                    return TSDateCalculator.TimeStepUnitCode.Month;

                // Now assume that the extracted string is the name of a TimeStepUnitCode enum.
                return (TSDateCalculator.TimeStepUnitCode)
                             Enum.Parse(typeof(TSDateCalculator.TimeStepUnitCode), substring, true);
            }
            catch
            {
                throw new TSLibraryException(ErrCode.Enum.Xml_Unit_Name_Unrecognized,
                            ReportedFileName + "contains an invalid <TimeStepUnit> element.\n\n" +
                            "'" + s + "' is not a recognized time step unit name.");
            }
        }
        
        #endregion


        #region Method Parse TimeStepQuantity()
        /// <summary>
        /// This method returns the TimeStepQuantity value from the given string.  The method
        /// will first attempt to parse the given string as an integer.
        /// If that fails, then it attempts to parse the given string as a HECDSS record
        /// name E part.  If that fails, then an exception is thrown.
        /// </summary>
        /// <param name="s">The string that is to be parsed to a TimeStepQuantity</param>
        /// <returns>TimeStepQuantity that results from the given string</returns>
        public short ParseTimeStepQuantity(String s)
        {
            try
            {   // First assume that the string is just an integer.
                // If that fails then we'll try some more complicated assumptions.
                return short.Parse(s);
            }
            catch { }

            try
            {
                // Assume that the string is a number followed by the unit name.
                // This pattern would be followed by data that came straight from 
                // HECDSS record name E part.
                String substring = Regex.Match(s, @"(\d)+").Value;

                // Now assume that the extracted string is the name of a TimeStepUnitCode enum.
                return short.Parse(substring);
            }
            catch
            {
                throw new TSLibraryException(ErrCode.Enum.Xml_Quantity_Unrecognized,
                            ReportedFileName + "contains an invalid <TimeStepQuantity> element.\n\n" +
                            "'" + s + "' can not be parsed.");
            }
        } 
        #endregion
    
    }

}
