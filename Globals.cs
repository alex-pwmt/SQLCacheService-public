﻿namespace SqlCacheService
{
	public static class Globals
	{
		public const string TextServiceName = "SqlCacheService";
		public const string LoggerTemplate = "{Time} SqlCacheService {ErrorMessage}";

		public const string LostContentError = "Socket client content was lost!";
		public const string SocketExceptionError = "SocketExceptionError: ";
		public const string ParseJsonError = "Parse Json request failed!";
		public const string ConfigError = "Service is not configured and can't run.";
		public const string FetchingDataError = "Fetching query data failed.";
		public const string UnrecognizedError = "Unrecognized command";
		public const string QueryNotFoundError = "Query not found";
		public const string FailedAddQueryError = "Failed to add query";
		public const string NotImplementedError = "Not implemented!";

		public const string ObjectDisposedException = "ObjectDisposedException: ";
		public const string JsonException = "JsonException: ";
		public const string FormatException = "FormatException: ";
		public const string Exception = "Exception: ";
	
		public const string TextSocketClosed = "Socket closed.";
		public const string TextListenerClosed = TextServiceName + " finished. Goodbye!";
		public const string TextDateFormat = "yyyy-MM-dd HH:mm:ss";
		public const string TextQueryAdded = "Query added";
		public const string TextLineEnd = "\n\r";
	}
}