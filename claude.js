import Anthropic from "@anthropic-ai/sdk";

const client = new Anthropic({
  apiKey: process.env.ANTHROPIC_API_KEY,
});

const prompt = process.argv.slice(2).join(" ");

const response = await client.messages.create({
  model: "claude-3-haiku-20240307",
  max_tokens: 500,
  messages: [{ role: "user", content: prompt }],
});

console.log(response.content[0].text);
