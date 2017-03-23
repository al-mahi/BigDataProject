package wekaproject;
/**
*<h1>clean input!</h1>
*@author Gautham
* This is a java class to takes JSON folder as input
* it will read all the json files in it and iterate it
* and convert it to an arff format used by the WEKA 
* <p>
* <b>Note:</b> some of the code in process has been commented.
* it is not redundant code. */
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

import weka.core.Stopwords;

import org.json.simple.parser.JSONParser;

public class cleanInput {
	/**
	 * The main method will take two urls as input and send it to 
	 * a method which will clean the data and convert into the arff format
	   *@param  listOfargs it will take a list of words as arguments
	   * @return void This method returns nothing
	   */
	public static void main(String[] args) throws IOException
	{
		String atffPath = "C:\\Users\\gauth\\Desktop\\output.txt";
		String jsonPath = "C:\\Users\\gauth\\Desktop\\twittwer_data";
		DataCleaner(atffPath, jsonPath);
		//implementWeka(atffPath);
//		String ssthInitialisationAndText[] = {"sentidata", "C:\\Users\\gauth\\workspace\\wekaproject\\src\\input",  "explain"};
//		SentiStrength.main(ssthInitialisationAndText); 
	}

//	private static void implementWeka(String atffPath) throws IOException
//	{
//		BufferedReader inputReader = null;
//
//		try
//		{
//			inputReader = new BufferedReader(new FileReader(atffPath));
//		} catch (FileNotFoundException ex)
//		{
//			System.err.println("File not found: " + atffPath);
//		}
//		try
//		{
//			Instances data = new Instances(inputReader);
//			data.setClassIndex(data.numAttributes() - 1);
//			Instances[][] split = new Instances[2][10];
//
//			for (int i = 0; i < 10; i++)
//			{
//				split[0][i] = data.trainCV(10, i);
//				split[1][i] = data.testCV(10, i);
//			}
//			Instances[] trainingSplits = split[0];
//			Instances[] testingSplits = split[1];
////			Classifier model=new NaiveBayes();
////			FastVector predictions = new FastVector();
////			for (int i = 0; i < trainingSplits.length; i++) {
////				Evaluation validation = classify(model, trainingSplits[i], testingSplits[i]);
////				 
////				predictions.appendElements(validation.predictions());
//// 
////				System.out.println(model.toString());
////			}
//			
//		} catch (Exception e)
//		{
//			e.printStackTrace();
//		}
//
//	}
	/**
	   * This method will clean the output. 
	   * most of pre prosessing is done before
	   * @param arffPath This is the path that the output should be written to.
	   * This is the input to the classifier.
	   * @param jsonPath this is the path where the json input resides
	   * @return Nothing.
	   * @exception Exception to handle all kinds of exceptions.
	   * @see IOException
	   */

	private static void DataCleaner(String atffPath, String jsonPath)
	{
		try
		{
			//start writing arff file
			PrintWriter writer = new PrintWriter(atffPath, "UTF-8");
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
			File dirr = new File(jsonPath);
			File[] files = dirr.listFiles();
			for (File file : files)
			{
				if (file.isFile())
				{
					//parse the json file using json object
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
					SimpleDateFormat twitterDateFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss ZZZZZ yyyy",
							Locale.ENGLISH);
					SimpleDateFormat wekatimestampformat = new SimpleDateFormat("dd-MM-yyyy-HH:mm");
					while ((line = bufferedReader.readLine()) != null)
					{
						try
						{
							//retrieve appropriate key values from json
							obj = parser.parse(line);
							jObj = (JSONObject) obj;
							id = jObj.get("id_str").toString();
							timestamp = jObj.get("created_at").toString();
							date = twitterDateFormat.parse(timestamp);
							timestamp = wekatimestampformat.format(date);
							message = jObj.get("text").toString();
							//send clean the message
							message = tokenizeandclean(message);
							//write output make sure that the string are in quotes
							writer.println(id + "," + timestamp + ",\'" + message+"\'");
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
/**
 *This method will tokenize the tweet words and clean it for the classifier
 * @param message A message is one single tweet
 * @return String of cleaned tweet
 */
	private static String tokenizeandclean(String message)
	{
		String wekafileredword;
		// takes care of escape characters in the tweets
		String[] tokens = message.split("\\W+");
		String cleanMessage = "";
		for (int i = 0; i < tokens.length; i++)
		{
			if (tokens[i].length() == 0)
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
			// if it is not English alphabet discard it
			else if (!tokens[i].matches("[a-zA-Z]+\\.?"))
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
					wekafileredword = cleanWithWeka(tokens[i].substring(1, tokens[i].length()));
					if (!wekafileredword.equals("-1"))
						cleanMessage += wekafileredword.toLowerCase() + " ";
				} else
				{
					wekafileredword = cleanWithWeka(tokens[i]);
					if (!wekafileredword.equals("-1"))
						cleanMessage += tokens[i].toLowerCase() + " ";
				}
			}
		}
		return cleanMessage;
	}
 /**
  * This method is used to remove the stop words with weka
  * @param word this is a single word in the tweet
  * @return if the word is not a stop word we return it else we return -1
  */
	private static String cleanWithWeka(String word)
	{		
		String cleansedword = word;
		Stopwords stpwrd = new Stopwords();
		// removing stop words
		if (stpwrd.is(word))
		{
			cleansedword = "-1";
		}
		return cleansedword;
	}
}
