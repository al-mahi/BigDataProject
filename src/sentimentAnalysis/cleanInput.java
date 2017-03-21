package wekaproject;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;
import org.json.simple.parser.JSONParser;

public class cleanInput {
	public static void main(String[] args) throws IOException
	{
		try
		{
			PrintWriter writer = new PrintWriter("C:\\Users\\gauth\\Desktop\\output.txt", "UTF-8");
			writer.println("@RELATION twitter");
			writer.println();
			writer.println();
			writer.println("@ATTRIBUTE id string");
			writer.println("@ATTRIBUTE time_stamp string");
			writer.println("@ATTRIBUTE tweet string");
			writer.println();
			writer.println();
			writer.println("@DATA");
			writer.println();
			File dirr = new File("C:\\Users\\gauth\\Desktop\\twittwer_data");
			File[] files = dirr.listFiles();
			for (File file : files)
			{
				if (file.isFile())
				{
					FileReader fileReader = new FileReader(file);
					BufferedReader bufferedReader = new BufferedReader(fileReader);
					String line;
					JSONParser parser = new JSONParser();
					Object obj;
					JSONObject jObj;
					String timestamp;
					String message;
					String id;
					Date date;
					int badRecordCounter = 0;
					SimpleDateFormat twitterDateFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss ZZZZZ yyyy", Locale.ENGLISH);
					SimpleDateFormat wekatimestampformat =new SimpleDateFormat("dd-MM-yyyy HH:mm");
					while ((line = bufferedReader.readLine()) != null)
					{
						try
						{
							obj = parser.parse(line);
							jObj = (JSONObject) obj;
							id = jObj.get("id_str").toString();
							timestamp = jObj.get("created_at").toString();
							date=twitterDateFormat.parse(timestamp);
							timestamp=wekatimestampformat.format(date);
							message = jObj.get("text").toString();
							message = tokenizeandclean(message);
//							System.out.println(id);
//							System.out.println(timestamp);
//							System.out.println(message);
//							System.out.println("****************");
							writer.println(id + "," + timestamp + "," + message);
						} catch (ParseException ex)
						{
							badRecordCounter++;
						}

					}

					if (badRecordCounter != 0)
					{
						System.out.println(badRecordCounter);
					}
					fileReader.close();
				}
			}
			writer.close();
		} catch (Exception ex)
		{
			ex.printStackTrace();
		}

	}

	private static String tokenizeandclean(String message)
	{
		// takes care of escape characters in the tweets
		String[] tokens = message.split("\\W+");
		String cleanMessage = "";
		for (int i = 0; i < tokens.length; i++)
		{
			if(tokens[i].length() == 0)
			{
				continue;
			}
			// taking care of escape characters in the tweet
			// tokens[i]=tokens[i].replace("\\", "");
			// removing user names			
			if (tokens[i].contains("@"))
			{
				// do nothing we do not need them.
			}
			// removing retweet text
			else if (tokens[i].equals("RT"))
			{
				// do nothing we do not need them.
			}
			// removing urls
			else if (tokens[i].contains("https://t.co"))
			{
				// do we do not need them.
			}
			// only words have meaning numbers are discarded
			else if (tokens[i].matches("[0-9]+"))
			{
				// do nothing
			}
			// these are the clean tokens need to keep them
			else
			{
				// hastags can have sentiments. removing the hash and retaining
				// tags
				if (tokens[i].substring(0, 1).equals("#"))
				{
					// remove hash
					cleanMessage += tokens[i].substring(1, tokens[i].length()) + " ";
				} else
					cleanMessage += tokens[i] + " ";
			}
		}
		return cleanMessage;
	}
}
