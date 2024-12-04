from openai import OpenAI
from config.config import config
def sentiment_analysis(comment: str) -> str:
    if comment:
        try:
            # Ensure API key is correctly set
            client = OpenAI(api_key=config['openai']['api_key'])

            # Call the API
            response = client.chat.completions.create(
                model='gpt-4o-mini',
                messages=[
                    {
                        "role": "system",
                        "content": (
                            "You are a machine learning model tasked with classifying comments "
                            "into POSITIVE, NEGATIVE, or NEUTRAL. Respond with one of these words only. "
                            "Here is the comment:\n\n{comment}"
                        ).format(comment=comment)
                    }
                ]
            )

            # Extract and validate the response
            sentiment = response.choices[0].message.content.strip()

            if sentiment in {"POSITIVE", "NEGATIVE", "NEUTRAL"}:
                return sentiment
            else:
                # Handle unexpected responses
                return "Error: Unexpected response"

        except Exception as e:
            # Handle any exceptions during API call
            return f"Error: {str(e)}"

    return "Empty"

testReview = "Amazingly amazing wings and homemade bleu cheese. Had the ribeye: tender, perfectly prepared, delicious. Nice selection of craft beers. Would DEFINITELY recommend checking out this hidden gem."

print(sentiment_analysis(testReview))

testReview = "Wow!  Yummy, different,  delicious.   Our favorite is the lamb curry and korma.  With 10 different kinds of naan!!!  Don't let the outside deter you (because we almost changed our minds)...go in and try something new!   You'll be glad you did!"

print(sentiment_analysis(testReview))
