using System.Collections;
using System.Linq.Expressions;
using System.Text.RegularExpressions;

namespace MQ_Sharp.Utils;

public static class Guard
{
    public static T NotNull<T>(T parameter, string paramName)
    {
        return parameter ?? throw new ArgumentNullException(paramName);
    }

    public static void Count(ICollection parameter, int length, string paramName)
    {
        if (parameter.Count != length)
        {
            throw new ArgumentOutOfRangeException(paramName, parameter.Count, string.Empty);
        }
    }

    public static void Greater(int parameter, int expected, string paramName)
    {
        if (parameter <= expected)
        {
            throw new ArgumentOutOfRangeException(paramName, parameter, string.Empty);
        }
    }

    public static void NotNullNorEmpty(string parameter, string paramName)
    {
        if (string.IsNullOrEmpty(parameter))
        {
            throw new ArgumentException("Given string is empty", paramName);
        }
    }

    public static void AllNotNull(IEnumerable parameter, string paramName)
    {
        foreach (var par in parameter)
        {
            if (par == null)
            {
                throw new ArgumentNullException(paramName);
            }
        }
    }

    public static void CheckBool(bool realValue, bool expected, string paramName)
    {
        if (realValue != expected)
        {
            throw new ArgumentException($"Parameter:{paramName} expect:{expected} real value:{realValue}");
        }
    }

    /// <summary>
    /// Checks whether given expression is true. Throws given exception type if not.
    /// </summary>
    /// <typeparam name="TException">
    /// Type of exception that i thrown when condition is not met.
    /// </typeparam>
    /// <param name="assertion">
    /// The assertion.
    /// </param>
    public static void Assert<TException>(Expression<Func<bool>> assertion)
        where TException : Exception, new()
    {
        var compiled = assertion.Compile();
        var evaluatedValue = compiled();
        if (!evaluatedValue)
        {
            var e = (Exception)Activator.CreateInstance(
                typeof(TException),
                new object[] { $"'{Normalize(assertion.ToString())}' is not met." })!;
            throw e;
        }
    }

    /// <summary>
    /// Creates string representation of lambda expression with unnecessary information 
    /// stripped out. 
    /// </summary>
    /// <param name="expression">Lambda expression to process. </param>
    /// <returns>Normalized string representation. </returns>
    private static string Normalize(string expression)
    {
        var result = expression;
        var replacements = new Dictionary<Regex, string>()
        {
            { new Regex("value\\([^)]*\\)\\."), string.Empty },
            { new Regex("\\(\\)\\."), string.Empty },
            { new Regex("\\(\\)\\ =>"), string.Empty },
            { new Regex("Not"), "!" }
        };

        foreach (var pattern in replacements)
        {
            result = pattern.Key.Replace(result, pattern.Value);
        }

        result = result.Trim();
        return result;
    }
}