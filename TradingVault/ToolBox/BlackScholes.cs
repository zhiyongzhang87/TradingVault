using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ToolBox
{
    public class BlackScholes
    {
        public BlackScholes()
        {

        }

        public double Price(ConstantValues.xOptionType argvOptionType, double argvUnderlyingPrice, double argvStrike, double argvTimeToMaturity, double argvYield
            , double argvRiskFreeRate, double argvVolatility)
        {
            double d1 = 0.0;
            double d2 = 0.0;
            double tPrice = 0.0;
            int tOptionTypeMultiplier = 1;

            if(argvOptionType == ConstantValues.xOptionType.Put)
            {
                tOptionTypeMultiplier = -1;
            }

            if (argvTimeToMaturity == 0)
            {
                tPrice = Math.Max((argvUnderlyingPrice - argvStrike) * tOptionTypeMultiplier, 0);
            }
            else if(argvTimeToMaturity < 0)
            {
                tPrice = 0;
            }
            else
            {
                d1 = (Math.Log(argvUnderlyingPrice / argvStrike) + (argvRiskFreeRate - argvYield + argvVolatility * argvVolatility / 2.0) * argvTimeToMaturity) 
                    / (argvVolatility * Math.Sqrt(argvTimeToMaturity));
                d2 = d1 - argvVolatility * Math.Sqrt(argvTimeToMaturity);
                tPrice = Math.Exp(-argvYield * argvTimeToMaturity) * tOptionTypeMultiplier * argvUnderlyingPrice * StandardNormalCumulativeDistribution(d1 * tOptionTypeMultiplier)
                        - Math.Exp(-argvRiskFreeRate * argvTimeToMaturity) * tOptionTypeMultiplier * argvStrike * StandardNormalCumulativeDistribution(d2 * tOptionTypeMultiplier);
            }

            return tPrice;
        }

        public double ImpliedVolatility(ConstantValues.xOptionType argvOptionType, double argvUnderlyingPrice, double argvStrike, double argvTimeToMaturity, double argvYield
            , double argvRiskFreeRate, double argvOptionPrice, double argvUpperVol)
        {
            double tImpliedVolatility = 0.0;
            double tUpperVol = argvUpperVol;
            double tLowerVol = 0.000001;
            double tPrice = this.Price(argvOptionType, argvUnderlyingPrice, argvStrike, argvTimeToMaturity, argvYield, argvRiskFreeRate, tUpperVol);

            if(argvOptionPrice > tPrice)
            {
                tImpliedVolatility = argvUpperVol;
            }
            else
            {
                while(tUpperVol - tLowerVol > 0.000001)
                {
                    tImpliedVolatility = (tUpperVol + tLowerVol) / 2;
                    tPrice = this.Price(argvOptionType, argvUnderlyingPrice, argvStrike, argvTimeToMaturity, argvYield, argvRiskFreeRate, tImpliedVolatility);
                    if(tPrice > argvOptionPrice)
                    {
                        tUpperVol = tImpliedVolatility;
                    }
                    else
                    {
                        tLowerVol = tImpliedVolatility;
                    }
                }

            }

            return tImpliedVolatility;
        }


        public double StandardNormalCumulativeDistribution(double argvValue)
        {
            double L = 0.0;
            double K = 0.0;
            double tReturnValue = 0.0;
            const double a1 = 0.31938153;
            const double a2 = -0.356563782;
            const double a3 = 1.781477937;
            const double a4 = -1.821255978;
            const double a5 = 1.330274429;
            L = Math.Abs(argvValue);
            K = 1.0 / (1.0 + 0.2316419 * L);
            tReturnValue = 1.0 - 1.0 / Math.Sqrt(2 * Convert.ToDouble(Math.PI.ToString())) *
                Math.Exp(-L * L / 2.0) * (a1 * K + a2 * K * K + a3 * Math.Pow(K, 3.0) +
                a4 * Math.Pow(K, 4.0) + a5 * Math.Pow(K, 5.0));

            if (argvValue < 0)
            {
                tReturnValue = 1.0 - tReturnValue;
            }

            return tReturnValue;
        }
    }
}
