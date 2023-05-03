import re
from mrjob.job import MRJob
from mrjob.protocol import BytesProtocol
import csv
import io


class MR_mapper_articles(MRJob):
    OUTPUT_PROTOCOL = BytesProtocol

    def mapper_init(self):
        self.article_title = ""
        self.in_body = False
        self.begin_check = False
        self.body = []
        self.cleaned_text = ""
        self.article_id = None
        self.related = False
        self.related_result = []
        self.article_date = ""
        self.cat = []
        self.subtitles = []
        self.infobox = False
        self.infobox_end = False
        self.checker = True
        self.clean_end = True

    def cleaner(self, body):
        cleaned_text = "".join(body)
        cleaned_text = re.sub(
            r"\&lt;div class\=.*?\&gt;|{\|.*?\|\}", " ", cleaned_text
        )  # remove tables type div
        cleaned_text = re.sub(
            r"\{\| class.*?\|\}|\{\|class.*?\|\}",
            " ",
            cleaned_text,
        )  # remove tables type normal
        cleaned_text = re.sub(
            r"\&lt;gallery.*?&lt;\/gallery\&gt;|\&lt;br\&gt;\&lt;gallery.*?\&lt;\/gallery\&gt;",
            " ",
            cleaned_text,
        )  # remove galleries
        cleaned_text = re.sub(
            r"\[\[.*?\|\&lt;nowiki\/\&gt;\]\]|\&lt;nowiki\/&gt;",
            " ",
            cleaned_text,
            re.IGNORECASE,
        )  # remove a pattern like this &lt;nowiki&gt;{{ship|HMCS|Kootenay|H75|4&lt;/nowiki&gt
        cleaned_text = re.sub(
            r"\{\{Weather box\|[^{}]*({{[^{}]*}}[^{}]*)*\}\}",
            " ",
            cleaned_text,
            flags = re.DOTALL|re.IGNORECASE
        )  # remove Weatherboxes
        cleaned_text = re.sub(
            r"\{\{Track listing [^{}]*({{[^{}]*}}[^{}]*)*\}\}",
            " ",
            cleaned_text
            #flags = re.DOTALL|re.IGNORECASE
        )  # remove Weatherboxes
        cleaned_text = re.sub(
            r"\[\[Image:.*?\]\]|\[\[File:.*?\]\]",
            " ",
            cleaned_text,
            flags=re.IGNORECASE,
        )  # remove remaining files and images (single brackets ]])
        cleaned_text = re.sub(
            r"\&lt\;nowiki\&gt.*?\&lt;\/nowiki\&gt;", " ", cleaned_text
        )  # remove nowwiki
        cleaned_text = re.sub(
            # r"\{\{efn[^{}]*({{[^{}]*}}[^{}]*)*\}\}", " ", cleaned_text
            r"\{\{efn[^\{\}]*(\{{[^\{\}]*}\}[^\{\}]*)*\}\}", " ", cleaned_text

        )  # remove nested curly brackets
        cleaned_text = re.sub(
            r"\&lt;.*?\&gt;.*?\&lt;\/.*?&gt;|\&lt;ref name.*?\&gt;|\&lt;\/ref.*?\/ref\&gt;|\&lt;ref.*?ref\&gt;|\&lt;ref.*?\/\&gt|\&lt;ref name.*?\/ref\&gt;|\{\{cite.*?\&lt;\/ref\&gt;|\&lt;ref.*?\/\&gt|\{\{cite.*?\}\}\&lt;\/ref\&gt;|\&lt;ref\&gt;.*?\&lt;\/ref\&gt",
            " ",
            cleaned_text,
            flags=re.IGNORECASE,
        )  # NEW remove references NEW (hello(world) (yes ))
        
        cleaned_text = re.sub(
            r"\&lt;math\&gt;.*math\&gt;|:\{\{math.*?\}\}\}\}", " ", cleaned_text
        )  # remove formula
        cleaned_text = re.sub(
            r"\&quot;", "", cleaned_text
        )  # remove quoat and quot signs
        # cleaned_text = re.sub(r"\{\{main\|.*?\}\}", " ", cleaned_text)                         #remove link to 'main' page after title
        cleaned_text = re.sub(
            r"\[\[[^\]\|]+\|([^\]]+)\]\]", r"\1", cleaned_text
        )  # remove hyperlinks to page with similar name, example [[city|cities]]
        cleaned_text = re.sub(
            r"\[http:\/\/.*? ", " ", cleaned_text
        )  # remove hyperlinks in text
        cleaned_text = re.sub(
            r"\&amp;nbsp;|\&amp", " ", cleaned_text
        )  # remove bold text
        cleaned_text = re.sub(r"(\w+)\.(\w+)", r"\1. \2", cleaned_text)  # remove
        cleaned_text = re.sub(
            r"\{\{clear\}\}", " ", cleaned_text
        )  # remove {{clear}} act like \newline
        cleaned_text = re.sub(
            r"\=+(.*?)\=+", r" \1. ", cleaned_text
        )  # replace titles ==titles== with title.
        cleaned_text = re.sub(
            r" \(\{\{IPA.*?\}\}\)", " ", cleaned_text
        )  # remove pronunciation
        if re.search(r"\{\{.+?'''(.+?)'''.+?\|.+?\}\}",cleaned_text):
            cleaned_text = re.sub(r"\{\{.+?'''(.+?)'''.+?\|.+?\}\}", r"\1", cleaned_text) # remove patterns like {{text|text}} but save the title if it is in there
        else:
            cleaned_text = re.sub(r"\{\{.+?\|.+?\}\}", " ", cleaned_text) # remove everything with pattern {{text|text}}

        cleaned_text = re.sub(
            r"\&lt;.*?\&gt;", " ", cleaned_text
        )  # fix instances like &lt; \&gt;

        cleaned_text = re.sub(
            r"\{\{ndash\}\}", "-", cleaned_text
        )  # replace {{ndah}} with -

        cleaned_text = re.sub(
            r"\s*\{\{.*?\}\}\s*", " ", cleaned_text
        )  # remove all  {{anything}}

        
        cleaned_text = re.sub(r"\[http.*?\/\/(.*?)\..*?\]", r"\1", cleaned_text) # remove website address inside the body like [https://kakasoftwares. com/top-10-baseball-players-in-the-world/ Baseball] is a bat-and-ball sport played on a field by two team s against each other. In ba

        # remove very general signs
        cleaned_text = re.sub(r"'''", " ", cleaned_text)  # remove triple quotes
        cleaned_text = re.sub(r"\[\[|\]\]", " ", cleaned_text)  # remove double [[]]
        cleaned_text = re.sub(r"\|\}|\{\|", " ", cleaned_text)  # remove {| and |}

        # remove ending of the article, if the ending is not proper
        last_dot_index = cleaned_text.rfind(".")  # find the index of the last full stop
        if last_dot_index != -1 and self.clean_end:
            cleaned_text = cleaned_text[: last_dot_index + 1]

        # make text little prettier again:
        cleaned_text = re.sub(r"  ", r" ", cleaned_text)
        cleaned_text = re.sub(r" \.", r".", cleaned_text)
        cleaned_text = re.sub(r" \,", r",", cleaned_text)
        cleaned_text = re.sub(r"\\u2013", "-", cleaned_text)
        cleaned_text=re.sub(r"\<sha1\>.*?\<page\>", "",cleaned_text)  #remove <sha1> lines
        return cleaned_text

    def deleter(self):
        self.body = []
        self.article_title = ""
        self.related_result = []
        self.cat = []
        self.article_date = "   "
        self.subtitles = []
        self.in_body = False  # for if the article has no references and related pages
        self.clean_end = True

    def yield_checker(self):
        self.checker = True
        if re.search("Category:|Module:", str(self.article_title)) or re.search(
            "Template:", str(self.article_title) 
        ) or re.search("MediaWiki:", str(self.article_title)):
            self.checker = False
        if not self.article_id or not self.body:
            self.checker = False

    def appender(self, line):
        append_checker = True
        if re.search(r"^\[\[(?:File|Image):(?: |)(.*)\]\]", line):
            append_checker = False
        if append_checker:
            self.body.append(line)

    def serializer(self, data):
        file = io.StringIO()
        writer = csv.writer(file)
        writer.writerow(data)

        return file.getvalue()[:-2].encode()

    def lister(self, self_var):
        # if self_var:
        self_var = ", ".join(str(x) for x in self_var)
        self_var = re.sub(r"\[|\]|\'|\|", "", self_var)
        # else:
        #    self_var = "None"
        return self_var

    def mapper(self, _, line):
        # for line in data_lines:
        line = line.strip()  # remove trailing/white space

        if line.find("<title>") == 0:
            self.yield_checker()
            if self.checker:
                self.cleaned_text =  self.cleaner(self.body)  #"".join(self.body) #
                data = [
                    self.article_id,
                    self.article_title,
                    self.article_date,
                    self.lister(self.cat),
                    self.lister(self.subtitles),
                    self.lister(self.related_result),
                    self.cleaned_text,
                ]
                yield self.serializer(data), None
            self.deleter()
            self.article_title = re.findall(r"<title>(.*?)</title>", line)

        elif line.find("<revision>") == 0:
            self.article_id = ""

        elif line.find("<id>") == 0 and self.article_id == "":
            self.article_id = line[4:-5]

        elif line.find("<timestamp") == 0:
            self.article_date = re.findall(r"\<timestamp\>(.*?)\<\/timestamp\>", line)

        elif line.find("<text bytes=") == 0:
            if re.search(
                "#REDIRECT", line, flags=re.IGNORECASE
            ):  # If the article title is merely a referal to another article
                self.deleter()
            elif re.search(
                "'''", line
            ):  # If the article starts on the same line as text bytes
                self.in_body = True
                line = re.sub(r"\<.*?\>", "", line)  # Remove textbytes text between <>
                self.body.append(line)
                self.begin_check = False
            elif re.search(
                r"\{\{.*?box",
                line,
                re.IGNORECASE,
            ):
                self.infobox = True
            else:
                self.begin_check = True

        elif (
            re.search(r"^\{\{.*?box", line)
            or line.lower().find("{{multiple image") == 0
            or line.lower().find("{{track listing")==0
        ):
            self.infobox = True
            self.begin_check = False
            self.in_body = False

        elif self.infobox:
            if self.infobox_end:
                if line.find("|") == 0 or line.find("*")==0:
                    self.infobox_end = False
                elif line == "}}":
                    pass
                else:
                    self.in_body = True
                    self.begin_check = False  # not necessary I think?
                    self.infobox = False
                    self.infobox_end = False
                    self.appender(line)
            if line == "}}":
                self.infobox_end = True

        elif (
            self.begin_check == True
            and line.find("redirects here. You may be looking for") == -1
            and line
        ):
            self.in_body = True
            self.begin_check = False
            self.appender(line)

        elif self.related:
            if not line:
                self.related = False
            else:
                self.related_result.append((re.findall(r"\* \[\[(.*?)\]\]", line)))

        elif line.find("[[Category:") == 0:  # To find categories
            self.cat.append((re.findall(r"\[\[Category:(.*?)\]\]", line)))

        elif re.search(r"\{\{.*?\}\}\<\/text\>", line):
            if line.find("stub") == -1:
                self.cat.append((re.findall(r"\{\{(.*?)\}\}", line)))

        elif self.in_body:
            if (
                line.find("==Related pages==") == 0
                or line.find("== Related pages ==") == 0
            ):
                self.related = True
                self.in_body = False
            elif (
                line.find("==References==") == 0
                or line.find("== References ==") == 0
                or line.find("==Other websites==") == 0
                or line.find("== Other websites ==") == 0
            ):
                self.in_body = False
                self.clean_end = False
            elif line.find("==") == 0 and line.find("===") == -1:
                self.subtitles.append(
                    self.cleaner(re.sub(r"\=\=(.*?)\=\=", r"\1", line))
                )
                self.appender(line)
            else:
                self.appender(line)

    # def combiner(self, key, values):
    #    yield key, values
    #
    # def reducer(self, key, values):
    #    yield key, values


if __name__ == "__main__":
    MR_mapper_articles.run()


# to do reduce or mapper
# exclude disambiguous
# exclude titles with category:
