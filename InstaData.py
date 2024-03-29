import pandas as pd
import requests
from time import sleep

def data_posts_count(searched_tag):
    'to get primary dataframe for the seareched tag and its total posts count'
    url = 'https://www.instagram.com/explore/tags/'+ searched_tag + '/?__a=1'
    response = requests.get(url, headers = {'User-agent': 'your bot 0.1'})
    data = response.json()
    total_posts_count = data['graphql']['hashtag']['edge_hashtag_to_media']['count']
    return data , total_posts_count

def extract_tags(x):
    'to extract hashtags/keywords from a text x if any'
    if x is pd.np.nan:
        return ''
    return ','.join(''.join([i for i in x.split() if i.startswith('#')]).split('#')[1:])

def get_temp_df(data):
    'to get temporary dataframe'
    # top_posts and other posts
    top_posts = data['graphql']['hashtag']['edge_hashtag_to_top_posts']['edges']
    posts = data['graphql']['hashtag']['edge_hashtag_to_media']['edges']
    # get the node where data located
    clean_top_posts = [i['node'] for i in top_posts ]
    clean_posts = [i['node'] for i in posts  ]
    # select the interested columns
    columns = ['owner', 'shortcode', 'taken_at_timestamp',  'edge_media_to_caption' , 'edge_media_to_comment', \
                       'edge_liked_by', 'is_video', 'video_view_count']
    # set up the dataframe
    top_posts_df = pd.DataFrame(clean_top_posts, columns = columns)
    top_posts_df['is_top'] = True
    posts_df = pd.DataFrame(clean_posts,columns = columns )
    posts_df['is_top'] = False
    temp_df = top_posts_df.append(posts_df, sort=False)
    return temp_df

def insta_scraper(searched_tag):
    """
    colllect the needed data for the interested hashtag
    ===================================================
    INPUT:
    searched_tag : str
    ---------------------------------------------------
    OUTPUT:
    {searched_tag}_data.csv file 
    data_df  : Pandas DataFrame with 
             -'user_id': user id number, str
             -'url':post link, str
             -'date': date of post , datetime64[ns]
             -'text': post text , str 
             -'num_comments': number of comments , int64
             -'num_likes': number of likes , int64
             -'is_video':is vedio post or not , bool
             -'video_view_count':number of vedio views if is_vedio: True , float64
             -'is_top': if the post is top post , bool
             -'other_tags' related hashtags/key words
    """
    # url
    url = 'https://www.instagram.com/explore/tags/'+ searched_tag + '/?__a=1'
    # get the previous data and total posts count
    data , total_posts_count = data_posts_count(searched_tag)
    print(f'There is {total_posts_count:,} posts for #{searched_tag}')
    # set up the dataframe
    data_df = get_temp_df(data)
    # has next page (True or False)
    has_next_page = data['graphql']['hashtag']['edge_hashtag_to_media']['page_info']['has_next_page']
    # get how many posts to scrape
    deep= int(input('Enter the number of posts you want to scrape:'))
    # get all data
    while has_next_page :
        if len(data_df)>= deep:
            break
        else:
            # end cursor to be put after (max_id = ) to go to next page
            next_page = data['graphql']['hashtag']['edge_hashtag_to_media']['page_info']['end_cursor']
            new_url = url + '&max_id=' + next_page
            # parse the interested url
            sleep(2)
            response = requests.get(new_url, headers = {'User-agent': 'your bot 0.1'})
            # data from json file
            data = response.json()            
            has_next_page = data['graphql']['hashtag']['edge_hashtag_to_media']['page_info']['has_next_page']
            temp_df = get_temp_df(data)
            data_df = temp_df.append(data_df,sort=False)
    data_df.reset_index(drop =True, inplace=True)
    
    # rename columns
    data_df.rename(columns={"edge_media_to_caption": "text", "edge_media_to_comment": "num_comments", \
                  "taken_at_timestamp":"date" , "edge_liked_by": "num_likes", \
                  "owner":"user_id", "is_video":"is_video",\
                  "video_view_count": "video_view_count" , 'shortcode':'url'},inplace=True)
    
    # fix the columns
    data_df.user_id = data_df.user_id.apply(lambda x: x['id'])
    data_df.url = data_df.url.apply(lambda x :'https://www.instagram.com/p/'+ x + '/')
    data_df.date = pd.to_datetime(data_df.date, unit='s')
    data_df.text = data_df.text.apply(lambda x: x['edges'][0]['node']['text'] if len(x['edges']) == 1 else pd.np.nan)
    data_df.num_comments = data_df.num_comments.apply(lambda x: x['count'])
    data_df.num_likes = data_df.num_likes.apply(lambda x: x['count'])
    # get other tags
    data_df['other_tags'] = data_df.text.apply(lambda x: extract_tags(x))
    
    #save to csv file to the working directory
    path_to_data = f'{searched_tag}_data.csv'
    # save data
    data_df.to_csv(path_to_data, encoding='utf-8-sig', index= False)
    return data_df

if __name__ == "__main__":
    tag = str(input('Enter the hashtag:'))
    print('Collecting data....')
    data = insta_scraper(tag)
    print('Data collection has been completed.')
