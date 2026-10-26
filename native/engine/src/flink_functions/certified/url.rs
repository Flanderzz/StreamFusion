use super::text;
use arrow::array::{ArrayRef, StringArray};
use std::sync::Arc;

pub(super) fn evaluate(args: &[ArrayRef]) -> Option<ArrayRef> {
    let mut output = Vec::with_capacity(args.first()?.len());
    let mut previous = None;
    for row in 0..args[0].len() {
        if args.iter().any(|a| a.is_null(row)) {
            output.push(None);
            continue;
        }
        let url = text(&args[0], row)?;
        let part = text(&args[1], row)?;
        if args.len() == 3 && part != "QUERY" {
            output.push(None);
            continue;
        }
        let value = match previous {
            Some((last_url, last_part, value)) if last_url == url && last_part == part => value,
            _ => {
                let value = parse(url, part)?;
                previous = Some((url, part, value));
                value
            }
        };
        output.push(if args.len() == 3 {
            let key = text(&args[2], row)?;
            if key.contains('&') {
                return None;
            }
            value.and_then(|query| {
                query
                    .split('&')
                    .find_map(|pair| pair.strip_prefix(key)?.strip_prefix('=').map(str::to_owned))
            })
        } else {
            value.map(str::to_owned)
        });
    }
    Some(Arc::new(StringArray::from(output)))
}

// Java URL retains raw case, escapes and dot segments. Restrict the certificate to
// hierarchical HTTP(S) URLs; normalization by a general URL parser changes these values.
fn parse<'a>(value: &'a str, part: &str) -> Option<Option<&'a str>> {
    if value.bytes().any(|b| b <= b' ' || b == b'\\') {
        return None;
    }
    let (protocol, rest) = value.split_once("://")?;
    if !matches!(protocol, "http" | "https") {
        return None;
    }
    let (without_ref, fragment) = rest
        .split_once('#')
        .map_or((rest, None), |(a, b)| (a, Some(b)));
    let (without_query, query) = without_ref
        .split_once('?')
        .map_or((without_ref, None), |(a, b)| (a, Some(b)));
    let (authority, path) = without_query.find('/').map_or((without_query, ""), |i| {
        (&without_query[..i], &without_query[i..])
    });
    if authority.is_empty() || authority.matches('@').count() > 1 {
        return None;
    }
    let (user, hostport) = authority
        .split_once('@')
        .map_or((None, authority), |(a, b)| (Some(a), b));
    if hostport.is_empty() || hostport.contains(['[', ']']) {
        return None;
    }
    let host = if let Some((host, port)) = hostport.split_once(':') {
        if host.is_empty()
            || host.contains(':')
            || port.is_empty()
            || !port.bytes().all(|b| b.is_ascii_digit())
            || port.parse::<i32>().is_err()
        {
            return None;
        }
        host
    } else {
        hostport
    };
    if !host
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'-' | b'_'))
    {
        return None;
    }
    Some(match part {
        "PROTOCOL" => Some(protocol),
        "HOST" => Some(host),
        "PATH" => Some(path),
        "QUERY" => query,
        "REF" => fragment,
        "AUTHORITY" => Some(authority),
        "USERINFO" => user,
        "FILE" => Some(&without_ref[authority.len()..]),
        _ => None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn raw_components_are_never_normalized() {
        let url = "http://user:pw@EXAMPLE.com:80/a/../b?q=a%2Bb+c&q=second#fragment";
        for (part, expected) in [
            ("HOST", "EXAMPLE.com"),
            ("PATH", "/a/../b"),
            ("FILE", "/a/../b?q=a%2Bb+c&q=second"),
            ("AUTHORITY", "user:pw@EXAMPLE.com:80"),
            ("USERINFO", "user:pw"),
            ("QUERY", "q=a%2Bb+c&q=second"),
        ] {
            assert_eq!(parse(url, part), Some(Some(expected)));
        }
        assert_eq!(parse("https://host", "PATH"), Some(Some("")));
        assert_eq!(parse("https://host?", "QUERY"), Some(Some("")));
        assert_eq!(parse(url, "host"), Some(None));
        for value in [
            "file:/a",
            "HTTP://host",
            "http://[::1]/",
            "http://host:bad/",
            " http://host",
        ] {
            assert!(parse(value, "HOST").is_none());
        }
    }
    #[test]
    fn first_raw_query_key_is_literal_and_not_decoded() {
        let args = [
            Arc::new(StringArray::from(vec!["http://h/?a.b=x+y&a.b=z"])) as ArrayRef,
            Arc::new(StringArray::from(vec!["QUERY"])) as ArrayRef,
            Arc::new(StringArray::from(vec!["a.b"])) as ArrayRef,
        ];
        assert_eq!(
            evaluate(&args).unwrap().as_ref(),
            &StringArray::from(vec!["x+y"])
        );
    }

    #[test]
    fn consecutive_components_reuse_only_the_same_url_and_part() {
        let args = [
            Arc::new(StringArray::from(vec![
                "http://h/a?x=1&y=2",
                "http://h/a?x=1&y=2",
                "http://h/a?x=1&y=2",
                "http://h/b",
            ])) as ArrayRef,
            Arc::new(StringArray::from(vec!["PATH", "PATH", "HOST", "PATH"])) as ArrayRef,
        ];
        assert_eq!(
            evaluate(&args).unwrap().as_ref(),
            &StringArray::from(vec!["/a", "/a", "h", "/b"])
        );
        let args = [
            Arc::new(StringArray::from(vec!["http://h/?x=1&y=2"; 2])) as ArrayRef,
            Arc::new(StringArray::from(vec!["QUERY"; 2])) as ArrayRef,
            Arc::new(StringArray::from(vec!["x", "y"])) as ArrayRef,
        ];
        assert_eq!(
            evaluate(&args).unwrap().as_ref(),
            &StringArray::from(vec!["1", "2"])
        );
    }
}
