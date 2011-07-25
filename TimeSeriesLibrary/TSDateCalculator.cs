using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace TimeSeriesLibrary
{
    class TSDateCalculator
    {
        public enum TimeStepUnitCode : short
        {
            Minute = 1,
            Hour = 2,
            Day = 3,
            Week = 4,
            Month = 5,
            Year = 6
        }

        public static DateTime IncrementDate(DateTime startDate, TimeStepUnitCode unit,
            short stepSize, int numSteps)
        {
            DateTime calcDate;

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

        public static int CountSteps(DateTime startDate, DateTime endDate,
            TimeStepUnitCode unit, short stepSize)
        {
            DateTime calcDate;
            int i=0;
            if (endDate < startDate)
            {
                calcDate = endDate;
                endDate = startDate;
                startDate = calcDate;
            }
            calcDate = startDate;

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
    }
}
