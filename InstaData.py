import json
import requests
import datetime
import pandas as pd
from time import sleep
import os

def hashtag_data(hashtag):
    """
    colllect the needed data for the interested hashtag
    ===================================================
    INPUT:
    hashtag : str
    ---------------------------------------------------
    OUTPUT:
    tags_data : Pandas DataFrame with 
               -hashtag : the name of the requested hashtag  ,str
               -related key words: related hashtags written in the post, list 
               -number of comments:
               -number of likes:
               -date of post: datetime
               -is_vedio : True or False
               -is_top : to be one of the top posts , True or False
               
    {hashtag}_data.csv file : convert pandas DataFrame to .csv file

    """
    # get the json link for the needed hashtag
    # get the variables from node .e inputed by the user
    hashtag = sys.argv[1]
    path = sys.argv[2] 
    url = 'https://www.instagram.com/explore/tags/'+ hashtag + '/?__a=1'
    response = requests.get(url)
    response_text = response.text
    data = json.loads(response_text)

    #loop through the old posts
    all_data = []
    all_data.append(data)
    
    has_next_page = data['graphql']['hashtag']['edge_hashtag_to_media']['page_info']['has_next_page']
    # get all data
    while has_next_page:
        # end cursor to be put after (max_id = ) to go to next page
        next_page = data['graphql']['hashtag']['edge_hashtag_to_media']['page_info']['end_cursor']
        new_url = url + '&max_id=' + next_page
        # parse the interested url
        sleep(2)
        response = requests.get(new_url)
        response_text = response.text
        # load json file
        data = json.loads(response_text)
        all_data.append(data)
        has_next_page = data['graphql']['hashtag']['edge_hashtag_to_media']['page_info']['has_next_page']

    #get post_details
    links,num_comments,num_likes,post_dates,is_vedios,is_tops,x_tags =[],[],[],[],[],[],[]
    #loop through all collected data
    for data in all_data:
        # arrange the data needed either from top_posts or normal posts 
        top_posts = data['graphql']['hashtag']['edge_hashtag_to_top_posts']['edges']
        posts = data['graphql']['hashtag']['edge_hashtag_to_media']['edges']
        all_posts = [top_posts,posts]
        for i in all_posts:
            if i is top_posts:
                # is_top
                is_top = True
            else:
                is_top = False
                
            # looping through both lists at once  
            for _ in i:
                # the post content
                post = _['node']
                # other hashtags/key words appear on the post
                try:
                    text = post['edge_media_to_caption']['edges'][0]['node']['text']
                    x_tag = [i for i in text.split() if i.startswith('#')] 
                except Exception:
                    continue
                # link to the post
                post_link = 'https://www.instagram.com/p/'+post['shortcode'] + '/'
                # number of comments on the post
                num_comment = post['edge_media_to_comment']['count']
                # number of likes 
                num_like = post['edge_liked_by']['count']
                # the post date
                post_date = datetime.datetime.fromtimestamp(post['taken_at_timestamp']).strftime("%Y-%m-%d")
                # vedio
                is_vedio = post['is_video']
                # gather other-tags data
                index = 0
                while index < len(x_tag):
                    x_tag[index] = x_tag[index].strip('#')
                    index += 1
                    x_tag_str = ','.join(x_tag)
                x_tags.append(x_tag_str)
                #if len(x_tag) > 0:
                #    x_tags.append(x_tag[0].split('\n')[-1])
                #else:
                #    x_tags.append('')
                links.append(post_link)
                num_comments.append(num_comment)
                num_likes.append(num_like)
                post_dates.append(post_date)
                is_vedios.append(is_vedio)
                is_tops.append(is_top)
                              
    tags_data = pd.DataFrame({'hashtag':hashtag,'other_tags':x_tags,'num_comments':num_comments, 
                          'num_likes':num_likes,'date':post_dates,'is_vedios':is_vedios,
                          'is_top_post':is_tops,'link':links})
    # replace the empty string eith none values
    tags_data.other_tags = tags_data.other_tags.apply(lambda x : None if x == '' else x)
    tags_data.other_tags = tags_data.other_tags.apply(lambda x : x.replace('#',','))
    # clean other_tags and create a nother dataFrame for other_tags
    tag_counter = Counter()
    for i in tags_data.other_tags:
        for tag in  i.split(','):
            tag_counter[tag] += 1

    other_tags_df = pd.DataFrame({'tag':list(tag_counter.keys()),'tag_count':list(tag_counter.values())})
    # replace empty string with null
    other_tags_df.tag = other_tags_df.tag.apply(lambda x : None if x == '' else x)
    # rearrange data by tag_count
    other_tags_df.sort_values(by='tag_count', ascending=False,inplace=True)

    #save to csv file to the working directory
    path_to_data = f'{hashtag}_data.csv'
    path_to_others = f'{hashtag}_related_tags.csv'
    # if needed to be saved to a certain directory uncomment the following
    path_to_hashtag = os.path.join(path,path_to_data) 
    path_to_related = os.path.join(path,path_to_others) 

    # save data
    tags_data.to_csv(path_to_hashtag, sep=',', header=True, encoding='utf-8', index= False)
    # save the other_hashtags data
    other_tags_df.to_csv(path_to_related, sep=',', header=True, encoding='utf-8',index =False)

    return tags_data , other_tags_df

if __name__ == "__main__":
    hashtag = str(input('Insert the needed hashatag...:'))
    print('Collecting data....')
    data, other_tags = hashtag_data(hashtag)
    print('Done...')
