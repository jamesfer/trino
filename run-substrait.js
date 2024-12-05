async function main() {
    const substraitJson = process.argv[2];

    console.log('Starting query')
    const response = await fetch('http://127.0.0.1:8080/v1/statement/substrait', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'X-Trino-User': 'James'
        },
        body: substraitJson,
    });
    if (!response.ok) {
        console.error('Failed to run query');
        console.error(await response.text());
        return;
    }

    const responseBody = await response.json();

    const data = [];
    let nextUri = responseBody.nextUri;
    while (nextUri) {
        console.log('Following up, current status: ' + responseBody.stats.state);
        const nextResponse = await fetch(nextUri, {
            headers: {
                'X-Trino-User': 'James'
            }
        });
        const nextResponseBody = await nextResponse.json();
        nextUri = nextResponseBody.nextUri;

        if (nextResponseBody.data) {
            data.push(...nextResponseBody.data);
        }

        if (nextResponseBody.error) {
            console.error('Query failed');
            console.error(nextResponseBody.error);
            return;
        }
    }

    console.log("Query complete");
    console.log(data);
}

main();
