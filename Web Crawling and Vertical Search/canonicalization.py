import re


class Canonicalizer:

    def get_domain(self, url: str):
        domain = re.findall("//[^/]*\w", url, flags=re.IGNORECASE)[0]
        domain = domain[2:]
        return domain

    def canonicalize(self, base_url: str, url: str):
        if "\\" in url:
            result = re.sub("\\\\+", "/", url.encode("unicode_escape").decode())
        else:
            result = url
        result = re.sub("[\n\t ]*", "", result)
        try:
            result = re.sub("#.*", "", result)
            if not re.findall("\w", result):
                return ""
            if re.match("^(?:\.{2}/)+\w+.*", result):
                replace = re.findall("\.{2}/\w+.*", result)[0][2:]
                level = len(re.findall("\.{2}", result))
                folders = re.findall("/\w+(?:\.\w+)*", base_url)
                target = "".join(folders[-level - 1:])
                result = re.sub(target, replace, base_url)
            black_list = [".jpg", ".svg", ".png", ".pdf", ".gif", "youtube", "edit", "footer", "sidebar", "cite",
                          "special", "mailto", ".webm", "tel:", "javascript", "www.vatican.va", ".ogv", "amazon"]
            for key in black_list:
                if key in result.lower():
                    return ""
            if re.match("https", result, flags=re.IGNORECASE) is not None:
                result = re.sub(":443", "", result)
            elif re.match("http", result, flags=re.IGNORECASE) is not None:
                result = re.sub(":80", "", result)
            result = re.sub("https", "http", result, flags=re.IGNORECASE)
            duplicate_slashes = re.findall("\w//+.", result)
            if len(duplicate_slashes) != 0:
                for dup in duplicate_slashes:
                    replace_str = dup[0] + "/" + dup[-1]
                    result = re.sub(dup, replace_str, result)
            find_domain = re.findall("//[^/]*\w", result)
            find_domain = find_domain[0]
            result = re.sub(find_domain, find_domain.lower(), result)
            return result
        except Exception:
            return ""
