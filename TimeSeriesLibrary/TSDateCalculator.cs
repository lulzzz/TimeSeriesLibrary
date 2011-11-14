using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace TimeSeriesLibrary
{
    /// <summary>
    /// Class stores methods and enum for computations involving time step size
    /// </summary>
    public class TSDateCalculator
    {
        /// <summary>
        /// enum expresses the possible values in the TimeStepUnit field of the database table
        /// </summary>
        public enum TimeStepUnitCode : short
        {
            Irregular = 0,
            Minute = 1,
            Hour = 2,
            Day = 3,
            Week = 4,
            Month = 5,
            Year = 6
        }

        /// <summary>
        /// Method computes the end date when given the start date, a regular time step size,
        /// and the number of time steps that should be added to the start date.
        /// </summary>
        /// <param name="startDate">The start date for the calculation</param>
        /// <param name="unit">Code for the time step units (e.g. hour, day, month...)</param>
        /// <param name="stepSize">The number of units per time step (e.g. Size=10 for 10-day time steps)</param>
        /// <param name="numSteps">The number of time steps to add to the start date</param>
        /// <returns>The end date of the calculation</returns>
        public static DateTime IncrementDate(DateTime startDate, TimeStepUnitCode unit,
            short stepSize, int numSteps)
        {
            DateTime calcDate;  // The end date of the calculation

            // The DateTime class's various 'Add' methods lend themselves to
            // a switch statement so that whichever units the time step is measured in,
            // we can call the corresponding 'Add' method.
            switch (unit)
            {
                case TimeStepUnitCode.Minute:
                    calcDate = startDate.AddMinutes(stepSize * numSteps);
                    break;
                case TimeStepUnitCode.Hour:
                    calcDate = startDate.AddHours(stepSize * numSteps);
                    break;
                case TimeStepUnitCode.Day:
                    calcDate = startDate.AddDays(stepSize * numSteps);
                    break;
                case TimeStepUnitCode.Week:
                    calcDate = startDate.AddDays(stepSize * numSteps * 7);
                    break;
                case TimeStepUnitCode.Month:
                    calcDate = startDate.AddMonths(stepSize * numSteps);
                    break;
                case TimeStepUnitCode.Year:
                    calcDate = startDate.AddYears(stepSize * numSteps);
                    break;
                default:
                    calcDate = startDate;
                    break;
            }
            return calcDate;
        }

        /// <summary>
        /// Method determines how many regular time steps are contained between the given
        /// start date and end date, when given a regular time step size.
        /// The method does not care whether the input start date comes before the input end
        /// date.  It simply returns the absolute value of the number of steps between the given dates.
        /// </summary>
        /// <param name="startDate">The date when the count begins</param>
        /// <param name="endDate">The date when the count ends</param>
        /// <param name="unit">Code for the time step units (e.g. hour, day, month...)</param>
        /// <param name="stepSize">The number of units per time step (e.g. Size=10 for 10-day time steps)</param>
        /// <returns></returns>
        public static int CountSteps(DateTime startDate, DateTime endDate,
            TimeStepUnitCode unit, short stepSize)
        {
            DateTime calcDate;  // Date value that will be iterated for the counting process
            int i=0;            // Counter for how many time steps
            
            // Method does not care whether the input start date comes before the input end
            // date.  It simply returns the absolute value of the number of steps between the given dates.
            if (endDate < startDate)
            {
                calcDate = endDate;
                endDate = startDate;
                startDate = calcDate;
            }
            calcDate = startDate;

            // The DateTime class's various 'Add' methods lend themselves to
            // a switch statement so that whichever units the time step is measured in,
            // we can call the corresponding 'Add' method.  The counting loop inside of
            // the switch statement is deliberately selected to be faster than a switch
            // statement inside of a loop.
            switch (unit)
            {
                case TimeStepUnitCode.Minute:
                    for (i = 0; calcDate < endDate; i++)
                        calcDate = calcDate.AddMinutes(stepSize);
                    break;
                case TimeStepUnitCode.Hour:
                    for (i = 0; calcDate < endDate; i++)
                        calcDate = calcDate.AddHours(stepSize);
                    break;
                case TimeStepUnitCode.Day:
                    for (i = 0; calcDate < endDate; i++)
                        calcDate = calcDate.AddDays(stepSize);
                    break;
                case TimeStepUnitCode.Week:
                    for (i = 0; calcDate < endDate; i++)
                        calcDate = calcDate.AddDays(stepSize * 7);
                    break;
                case TimeStepUnitCode.Month:
                    for (i = 0; calcDate < endDate; i++)
                        calcDate = calcDate.AddMonths(stepSize);
                    break;
                case TimeStepUnitCode.Year:
                    for (i = 0; calcDate < endDate; i++)
                        calcDate = calcDate.AddYears(stepSize);
                    break;
            }
            return i;
        }

        public static void FillDateArray(
                    TimeStepUnitCode timeStepUnit, short timeStepQuantity,
                    int nReqValues, DateTime[] dateArray, DateTime reqStartDate)
        {
            dateArray[0] = reqStartDate;
            // Loop through the length of the array and fill in the date values
            for (int i = 1; i < nReqValues; i++)
            {
                dateArray[i] = IncrementDate(dateArray[i - 1], timeStepUnit, timeStepQuantity, 1);
            }
        }

    }
}
